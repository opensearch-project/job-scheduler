/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.spi.StatusHistoryModel;
import org.opensearch.transport.client.Client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

public class JobHistoryService {
    private static final Logger logger = LogManager.getLogger(JobHistoryService.class);
    public static final String JOB_HISTORY_INDEX_NAME = ".job-scheduler-history";

    private final Client client;
    private final ClusterService clusterService;
    final static Map<String, Object> INDEX_SETTINGS = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-1");

    public JobHistoryService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    private String historyMapping() {
        try {
            InputStream in = JobHistoryService.class.getResourceAsStream("/mappings/job_scheduler_history.json");
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null;) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("History Mapping cannot be read correctly.");
        }
    }

    public boolean historyIndexExist() {
        return clusterService.state().routingTable().hasIndex(JOB_HISTORY_INDEX_NAME);
    }

    void createHistoryIndex(ActionListener<Boolean> listener) {
        if (historyIndexExist()) {
            listener.onResponse(true);
        } else {
            final CreateIndexRequest request = new CreateIndexRequest(JOB_HISTORY_INDEX_NAME).mapping(
                historyMapping(),
                (MediaType) XContentType.JSON
            ).settings(INDEX_SETTINGS);
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
    }

    /**
     * Records job execution history to the history index.
     *
     * @param startTime the time when job execution started
     * @param endTime the time when job execution ended (can be null for ongoing jobs)
     * @param status the execution status (e.g., "RUNNING", "SUCCESS", "FAILED")
     * @param listener callback for handling the response
     */
    public void recordJobHistory(
        final String jobIndexName,
        final String jobId,
        final Instant startTime,
        final Instant endTime,
        final Integer status,
        ActionListener<Boolean> listener
    ) {
        if (jobIndexName == null || jobId == null || startTime == null) {
            listener.onFailure(new IllegalArgumentException("JobIndexName, JobId, StartTime, and Status cannot be null"));
            return;
        }

        createHistoryIndex(ActionListener.wrap(created -> {
            if (created) {
                findHistoryRecord(jobIndexName, jobId, startTime, ActionListener.wrap(existingRecord -> {
                    if (existingRecord != null) {
                        // Update existing record
                        StatusHistoryModel updatedModel = new StatusHistoryModel(jobIndexName, jobId, startTime, endTime, status);
                        updateHistoryRecord(
                            updatedModel,
                            ActionListener.wrap(updated -> listener.onResponse(updated != null), listener::onFailure)
                        );
                    } else {
                        // Create new record
                        try {
                            StatusHistoryModel historyModel = new StatusHistoryModel(jobIndexName, jobId, startTime, endTime, status);
                            createHistoryRecord(historyModel, listener);
                        } catch (Exception e) {
                            logger.error("Failed to create history record", e);
                            listener.onFailure(e);
                        }
                    }
                }, listener::onFailure));
            } else {
                listener.onResponse(false);
            }
        }, listener::onFailure));
    }

    private void createHistoryRecord(final StatusHistoryModel historyModel, ActionListener<Boolean> listener) {
        try {
            String historyId = generateHistoryId(historyModel.getJobIndexName(), historyModel.getJobId(), historyModel.getStartTime());

            final IndexRequest request = new IndexRequest(JOB_HISTORY_INDEX_NAME).id(historyId)
                .source(historyModel.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .create(true);

            client.index(request, ActionListener.wrap(response -> {
                logger.debug("Successfully recorded job history: {}", historyModel);
                listener.onResponse(true);
            }, exception -> {
                if (exception instanceof VersionConflictEngineException) {
                    logger.debug("History record already exists: {}", exception.getMessage());
                }
                listener.onFailure(exception);
            }));
        } catch (IOException e) {
            logger.error("IOException occurred creating history record", e);
            listener.onFailure(e);
        }
    }

    public void updateHistoryRecord(final StatusHistoryModel historyModelupdate, ActionListener<StatusHistoryModel> listener) {
        try {
            String documentId = generateHistoryId(
                historyModelupdate.getJobIndexName(),
                historyModelupdate.getJobId(),
                historyModelupdate.getStartTime()
            );

            UpdateRequest updateRequest = new UpdateRequest().index(JOB_HISTORY_INDEX_NAME)
                .id(documentId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .doc(historyModelupdate.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            client.update(
                updateRequest,
                ActionListener.wrap(
                    response -> listener.onResponse(
                        new StatusHistoryModel(
                            historyModelupdate.getJobIndexName(),
                            historyModelupdate.getJobId(),
                            historyModelupdate.getStartTime(),
                            historyModelupdate.getEndTime(),
                            historyModelupdate.getStatus()
                        )
                    ),
                    exception -> {
                        if (exception instanceof VersionConflictEngineException) {
                            logger.debug("Version conflict updating history record: {}", exception.getMessage());
                        }
                        listener.onResponse(null);
                    }
                )
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    public void findHistoryRecord(
        final String jobIndexName,
        final String jobId,
        final Instant startTime,
        ActionListener<StatusHistoryModel> listener
    ) {
        String historyId = generateHistoryId(jobIndexName, jobId, startTime);
        GetRequest getRequest = new GetRequest(JOB_HISTORY_INDEX_NAME).id(historyId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onResponse(null);
            } else {
                try {
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                    parser.nextToken();
                    listener.onResponse(StatusHistoryModel.parse(parser));
                } catch (IOException e) {
                    logger.error("IOException occurred parsing history record", e);
                    listener.onResponse(null);
                }
            }
        }, exception -> {
            if (exception.getMessage() != null && exception.getMessage().contains("no such index")) {
                listener.onResponse(null);
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private String generateHistoryId(String jobIndexName, String jobId, Instant startTime) {
        return jobIndexName + "-" + jobId + "-" + startTime.getEpochSecond();
    }
}
