/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi.utils;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import com.cronutils.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.transport.client.Client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class LockService {
    private static final Logger logger = LogManager.getLogger(LockService.class);
    public static final String LOCK_INDEX_NAME = ".opendistro-job-scheduler-lock";

    private final Client client;
    private final ClusterService clusterService;

    // This is used in tests to control time.
    private Instant testInstant = null;

    public LockService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    private String lockMapping() {
        try {
            InputStream in = LockService.class.getResourceAsStream("opensearch_job_scheduler_lock.json");
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null;) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("Lock Mapping cannot be read correctly.");
        }
    }

    public boolean lockIndexExist() {
        return clusterService.state().routingTable().hasIndex(LOCK_INDEX_NAME);
    }

    @VisibleForTesting
    void createLockIndex(ActionListener<Boolean> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            if (lockIndexExist()) {
                listener.onResponse(true);
            } else {
                final CreateIndexRequest request = new CreateIndexRequest(LOCK_INDEX_NAME).mapping(
                    lockMapping(),
                    (MediaType) XContentType.JSON
                );
                client.admin()
                    .indices()
                    .create(request, ActionListener.wrap(response -> listener.onResponse(response.isAcknowledged()), exception -> {
                        if (exception instanceof ResourceAlreadyExistsException
                            || exception.getCause() instanceof ResourceAlreadyExistsException) {
                            listener.onResponse(true);
                        } else {
                            listener.onFailure(exception);
                        }
                    }));
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Attempts to acquire lock the job. If the lock does not exists it attempts to create the lock document.
     * If the Lock document exists, it will try to update and acquire lock.
     *
     * @param jobParameter a {@code ScheduledJobParameter} containing the lock duration.
     * @param context a {@code JobExecutionContext} containing job index name and job id.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the lock if it was acquired
     *                 or else null. Passes {@code IllegalArgumentException} to onFailure if the {@code ScheduledJobParameter} does not
     *                 have {@code LockDurationSeconds}.
     */
    public void acquireLock(
        final ScheduledJobParameter jobParameter,
        final JobExecutionContext context,
        ActionListener<LockModel> listener
    ) {
        final String jobIndexName = context.getJobIndexName();
        final String jobId = context.getJobId();
        final long lockDurationSeconds = jobParameter.getLockDurationSeconds();
        acquireLockWithId(jobIndexName, lockDurationSeconds, jobId, listener);
    }

    /**
     * Attempts to acquire a lock with a specific lock Id. If the lock does not exist it attempts to create the lock document.
     * If the Lock document exists, it will try to update and acquire the lock.
     *
     * @param jobIndexName a non-null job index name.
     * @param lockDurationSeconds the amount of time in seconds that the lock should exist
     * @param lockId the unique Id for the lock. This should represent the resource that the lock is on, whether it be
     *               a job, or some other arbitrary resource. If the lockID matches a jobID, then the lock will be deleted
     *               when the job is deleted.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the lock if it was acquired
     *                 or else null. Passes {@code IllegalArgumentException} to onFailure if the {@code ScheduledJobParameter} does not
     *                 have {@code LockDurationSeconds}.
     */
    public void acquireLockWithId(
        final String jobIndexName,
        final Long lockDurationSeconds,
        final String lockId,
        ActionListener<LockModel> listener
    ) {
        if (lockDurationSeconds == null) {
            listener.onFailure(new IllegalArgumentException("Job LockDuration should not be null"));
        } else if (jobIndexName == null) {
            listener.onFailure(new IllegalArgumentException("Job index name should not be null"));
        } else if (lockId == null) {
            listener.onFailure(new IllegalArgumentException("Lock ID should not be null"));
        } else {
            createLockIndex(ActionListener.wrap(created -> {
                if (created) {
                    try {
                        findLock(LockModel.generateLockId(jobIndexName, lockId), ActionListener.wrap(existingLock -> {
                            if (existingLock != null) {
                                if (isLockReleasedOrExpired(existingLock)) {
                                    // Lock is expired. Attempt to acquire lock.
                                    logger.debug("lock is released or expired: " + existingLock);
                                    LockModel updateLock = new LockModel(existingLock, getNow(), lockDurationSeconds, false);
                                    updateLock(updateLock, listener);
                                } else {
                                    logger.debug("Lock is NOT released or expired. " + existingLock);
                                    // Lock is still not expired. Return null as we cannot acquire lock.
                                    listener.onResponse(null);
                                }
                            } else {
                                // There is no lock object and it is first time. Create new lock.
                                // Note that the lockID will be set to {jobIndexName}-{lockId}
                                LockModel tempLock = new LockModel(jobIndexName, lockId, getNow(), lockDurationSeconds, false);
                                logger.debug("Lock does not exist. Creating new lock" + tempLock);
                                createLock(tempLock, listener);
                            }
                        }, listener::onFailure));
                    } catch (VersionConflictEngineException e) {
                        logger.debug("could not acquire lock {}", e.getMessage());
                        listener.onResponse(null);
                    }
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }
    }

    private boolean isLockReleasedOrExpired(final LockModel lock) {
        return lock.isReleased() || lock.isExpired();
    }

    private void updateLock(final LockModel updateLock, ActionListener<LockModel> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            UpdateRequest updateRequest = new UpdateRequest().index(LOCK_INDEX_NAME)
                .id(updateLock.getLockId())
                .setIfSeqNo(updateLock.getSeqNo())
                .setIfPrimaryTerm(updateLock.getPrimaryTerm())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .doc(updateLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            client.update(
                updateRequest,
                ActionListener.wrap(
                    response -> listener.onResponse(new LockModel(updateLock, response.getSeqNo(), response.getPrimaryTerm())),
                    exception -> {
                        if (exception instanceof VersionConflictEngineException) {
                            logger.debug("could not acquire lock {}", exception.getMessage());
                        }
                        if (exception instanceof DocumentMissingException) {
                            logger.debug(
                                "Document is deleted. This happens if the job is already removed and" + " this is the last run." + "{}",
                                exception.getMessage()
                            );
                        }
                        if (exception instanceof IOException) {
                            logger.error("IOException occurred updating lock.", exception);
                        }
                        listener.onResponse(null);
                    }
                )
            );
        } catch (IOException e) {
            logger.error("IOException occurred updating lock.", e);
            listener.onResponse(null);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void createLock(final LockModel tempLock, ActionListener<LockModel> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            final IndexRequest request = new IndexRequest(LOCK_INDEX_NAME).id(tempLock.getLockId())
                .source(tempLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                .setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .create(true);
            client.index(
                request,
                ActionListener.wrap(
                    response -> listener.onResponse(new LockModel(tempLock, response.getSeqNo(), response.getPrimaryTerm())),
                    exception -> {
                        if (exception instanceof VersionConflictEngineException) {
                            logger.debug("Lock is already created. {}", exception.getMessage());
                            listener.onResponse(null);
                            return;
                        }
                        listener.onFailure(exception);
                    }
                )
            );
        } catch (IOException e) {
            logger.error("IOException occurred creating lock", e);
            listener.onFailure(e);
        }
    }

    public void findLock(final String lockId, ActionListener<LockModel> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            GetRequest getRequest = new GetRequest(LOCK_INDEX_NAME).id(lockId);
            client.get(getRequest, ActionListener.wrap(response -> {
                if (!response.isExists()) {
                    listener.onResponse(null);
                } else {
                    try {
                        XContentParser parser = XContentType.JSON.xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                        parser.nextToken();
                        listener.onResponse(LockModel.parse(parser, response.getSeqNo(), response.getPrimaryTerm()));
                    } catch (IOException e) {
                        logger.error("IOException occurred finding lock", e);
                        listener.onResponse(null);
                    }
                }
            }, exception -> {
                logger.error("Exception occurred finding lock", exception);
                listener.onFailure(exception);
            }));
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Attempt to release the lock.
     * Most failure cases are due to {@code lock.seqNo} and {@code lock.primaryTerm} not matching with the existing
     * document.
     *
     * @param lock a {@code LockModel} to be released.
     * @param listener a {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the release was successful
     */
    public void release(final LockModel lock, ActionListener<Boolean> listener) {
        if (lock == null) {
            logger.debug("Lock is null. Nothing to release.");
            listener.onResponse(false);
        } else {
            logger.debug("Releasing lock: " + lock);
            final LockModel lockToRelease = new LockModel(lock, true);
            updateLock(lockToRelease, ActionListener.wrap(releasedLock -> listener.onResponse(releasedLock != null), listener::onFailure));
        }
    }

    /**
     * Attempt to delete lock.
     * This should be called as part of clean up when the job for corresponding lock is deleted.
     *
     * @param lockId a {@code String} to be deleted.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the delete was successful
     */
    public void deleteLock(final String lockId, ActionListener<Boolean> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            DeleteRequest deleteRequest = new DeleteRequest(LOCK_INDEX_NAME).id(lockId);
            client.delete(deleteRequest, ActionListener.wrap(response -> {
                listener.onResponse(
                    response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND
                );
            }, exception -> {
                if (exception instanceof IndexNotFoundException || exception.getCause() instanceof IndexNotFoundException) {
                    logger.debug("Index is not found to delete lock. {}", exception.getMessage());
                    listener.onResponse(true);
                } else {
                    listener.onFailure(exception);
                }
            }));
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Attempt to renew a lock.
     * It is used to give an extended valid period to a lock. The start time of the lock will be updated to
     * the current time when the method get called, and the duration of the lock remains.
     * It works as long as the lock is not acquired by others, and no matter if the lock is expired of not.
     *
     * @param lock a {@code LockModel} to be renewed.
     * @param listener a {@code ActionListener} that has onResponse and onFailure that is used to
     *                 return the renewed lock if renewal succeed, otherwise return null.
     */
    public void renewLock(final LockModel lock, ActionListener<LockModel> listener) {
        if (lock == null) {
            logger.debug("Lock is null. Nothing to renew.");
            listener.onResponse(null);
        } else {
            logger.debug(
                "Renewing lock: {}. The lock was acquired or renewed on: {}, and the duration was {} sec.",
                lock,
                lock.getLockTime(),
                lock.getLockDurationSeconds()
            );
            final LockModel lockToRenew = new LockModel(lock, getNow(), lock.getLockDurationSeconds(), false);
            updateLock(lockToRenew, ActionListener.wrap(renewedLock -> {
                logger.debug(
                    "Renewed lock: {}. It is supposed to be valid for another {} sec from {}.",
                    renewedLock,
                    renewedLock.getLockDurationSeconds(),
                    renewedLock.getLockTime()
                );
                listener.onResponse(renewedLock);
            }, exception -> {
                logger.debug("Failed to renew lock: {}.", lock);
                listener.onFailure(exception);
            }));
        }
    }

    private Instant getNow() {
        return testInstant != null ? testInstant : Instant.now();
    }

    @VisibleForTesting
    void setTime(final Instant testInstant) {
        this.testInstant = testInstant;
    }
}
