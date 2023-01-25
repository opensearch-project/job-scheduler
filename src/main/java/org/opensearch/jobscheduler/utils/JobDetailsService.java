/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.action.ExtensionProxyAction;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.JobParameterRequest;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Map;

public class JobDetailsService implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(JobDetailsService.class);
    public static final String JOB_DETAILS_INDEX_NAME = ".opensearch-job-scheduler-job-details";
    private static final String PLUGINS_JOB_DETAILS_MAPPING_FILE = "/mappings/opensearch_job_scheduler_job_details.json";

    public static Long TIME_OUT_FOR_REQUEST = 10L;
    private final Client client;
    private final ClusterService clusterService;
    private Set<String> indicesToListen;
    private Map<String, ScheduledJobProvider> indexToJobProviders;
    private static final ConcurrentMap<String, JobDetails> indexToJobDetails = IndexToJobDetails.getInstance();

    public JobDetailsService(
        final Client client,
        final ClusterService clusterService,
        Set<String> indicesToListen,
        Map<String, ScheduledJobProvider> indexToJobProviders
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesToListen = indicesToListen;
        this.indexToJobProviders = indexToJobProviders;
    }

    public static ConcurrentMap<String, JobDetails> getIndexToJobDetails() {
        return JobDetailsService.indexToJobDetails;
    }

    public boolean jobDetailsIndexExist() {
        return clusterService.state().routingTable().hasIndex(JOB_DETAILS_INDEX_NAME);
    }

    private void updateIndicesToListen(String jobIndexName) {
        this.indicesToListen.add(jobIndexName);
    }

    /**
     * Creates a proxy {@link ScheduledJobProvier} that facilitates callbacks between extensions and JobScheduler
     *
     * @param jobDetails the extension job information
     */
    private void updateIndexToJobProviders(JobDetails jobDetails) {

        // Extract jobIndex and jobType
        String extensionJobIndexName = jobDetails.getJobIndex();
        String extensionJobTypeName = jobDetails.getJobType();

        // Extract proxy actions
        String extensionJobParameterAction = jobDetails.getJobParameterAction();
        String extensionJobRunnerAction = jobDetails.getJobRunnerAction();

        // Create proxy ScheduledJobParser
        ScheduledJobParser extensionJobParser = new ScheduledJobParser() {

            @Override
            public ScheduledJobParameter parse(XContentParser xContentParser, String id, JobDocVersion jobDocVersion) throws IOException {

                final ExtensionJobParameter[] extensionJobParameterHolder = new ExtensionJobParameter[1];
                CompletableFuture<ExtensionJobParameter[]> inProgressFuture = new CompletableFuture<>();

                // TODO : Replace the placeholder with the provided access token from the inital job detials request
                // Prepare JobParameterRequest
                JobParameterRequest jobParamRequest = new JobParameterRequest("placeholder", xContentParser, id, jobDocVersion);

                // Invoke extension job parameter action and return ScheduledJobParameter
                client.execute(
                    ExtensionProxyAction.INSTANCE,
                    new ExtensionJobActionRequest<JobParameterRequest>(extensionJobRunnerAction, jobParamRequest),
                    ActionListener.wrap(response -> {

                        // Extract response bytes into a streamInput and set the extensionJobParameter
                        StreamInput in = StreamInput.wrap(response.getResponseBytes());
                        extensionJobParameterHolder[0] = new ExtensionJobParameter(in);
                        inProgressFuture.complete(extensionJobParameterHolder);

                    }, exception -> {
                        logger.error("Could not parse job parameter", exception);
                        inProgressFuture.completeExceptionally(exception);
                    })
                );

                // Stall execution until request completes or times out
                try {
                    inProgressFuture.orTimeout(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS).join();
                } catch (CompletionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        logger.info("Request timed out with an exception ", e);
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    logger.info("Could not parse ScheduledJobParameter due to exception ", e);
                }

                return extensionJobParameterHolder[0];
            }
        };

        // Create proxy ScheduledJobRunner
        ScheduledJobRunner extensionJobRunner = new ScheduledJobRunner() {
            @Override
            public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {

                CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();

                try {
                    // TODO : Replace the placeholder with the provided access token from the inital job detials request
                    // Prepare JobRunnerRequest
                    JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("placeholder", jobParameter, context);
                    // Invoke extension job runner action
                    client.execute(
                        ExtensionProxyAction.INSTANCE,
                        new ExtensionJobActionRequest<JobRunnerRequest>(extensionJobRunnerAction, jobRunnerRequest),
                        ActionListener.wrap(response -> {

                            // Extract response bytes into a streamInput and set the extensionJobParameter
                            StreamInput in = StreamInput.wrap(response.getResponseBytes());
                            inProgressFuture.complete(in.readBoolean());

                        }, exception -> {
                            logger.error("Failed to run job due to exception ", exception);
                            inProgressFuture.completeExceptionally(exception);
                        })
                    );
                } catch (IOException e) {
                    logger.error("Failed to create JobRunnerRequest", e);
                }
            }
        };

        // Update indexToJobProviders
        this.indexToJobProviders.put(
            extensionJobTypeName,
            new ScheduledJobProvider(extensionJobTypeName, extensionJobIndexName, extensionJobParser, extensionJobRunner)
        );
    }

    /**
     * Adds a new entry into the indexToJobDetails using the document Id as the key, registers the index name to indicesToListen, and registers the ScheduledJobProvider
     *
     * @param documentId the unique Id for the job details
     * @param jobDetails the jobDetails to register
     */
    void updateIndexToJobDetails(String documentId, JobDetails jobDetails) {
        // Register new JobDetails entry
        indexToJobDetails.put(documentId, jobDetails);
        updateIndicesToListen(jobDetails.getJobIndex());
        updateIndexToJobProviders(jobDetails);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {

        // Determine if index operation was successful
        if (result.getResultType().equals(Engine.Result.Type.FAILURE)) {
            logger.info("Job Details Registration failed for extension {} on index {}", index.id(), shardId.getIndexName());
            return;
        }

        // Generate parser using bytesRef from index
        try {
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, index.source().utf8ToString());
            parser.nextToken();
            updateIndexToJobDetails(index.id(), JobDetails.parse(parser));
        } catch (IOException e) {
            logger.error("IOException occurred creating job details for extension id " + index.id(), e);
        }
    }

    /**
     *
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details index if it was created
     *                 or else null.
     */
    @VisibleForTesting
    void createJobDetailsIndex(ActionListener<Boolean> listener) {
        if (jobDetailsIndexExist()) {
            listener.onResponse(true);
        } else {
            CreateIndexRequest request = new CreateIndexRequest(JOB_DETAILS_INDEX_NAME).mapping(jobDetailsMapping());
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
     * Attempts to process job details with a specified documentId. If the job details does not exist it attempts to create the job details document.
     * If the job details document exists, it will try to update the job details.
     *
     * @param documentId a nullable document Id
     * @param jobIndexName a non-null job index name.
     * @param jobTypeName a non-null job type name.
     * @param jobParameterActionName a non-null job parameter action name.
     * @param jobRunnerActionName a non-null job runner action name.
     * @param extensionUniqueId the extension Id
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was processed
     *                 or else null.
     */
    public void processJobDetails(
        final String documentId,
        final String jobIndexName,
        final String jobTypeName,
        final String jobParameterActionName,
        final String jobRunnerActionName,
        final String extensionUniqueId,
        ActionListener<String> listener
    ) {
        // Validate job detail params
        if (jobIndexName == null
            || jobIndexName.isEmpty()
            || jobTypeName == null
            || jobTypeName.isEmpty()
            || jobParameterActionName == null
            || jobParameterActionName.isEmpty()
            || jobRunnerActionName == null
            || jobRunnerActionName.isEmpty()
            || extensionUniqueId == null
            || extensionUniqueId.isEmpty()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "JobIndexName, JobTypeName, JobParameterActionName, JobRunnerActionName, Extension Unique Id must not be null or empty"
                )
            );
        } else {
            // Ensure job details index has been created
            createJobDetailsIndex(ActionListener.wrap(created -> {
                if (created) {
                    try {
                        // Update entry request
                        if (documentId != null) {
                            // Recover entry via documentId
                            findJobDetails(documentId, ActionListener.wrap(existingJobDetails -> {
                                JobDetails updateJobDetails = new JobDetails(existingJobDetails);

                                // Set updated fields
                                updateJobDetails.setJobIndex(jobIndexName);
                                updateJobDetails.setJobType(jobTypeName);
                                updateJobDetails.setJobParameterAction(jobParameterActionName);
                                updateJobDetails.setJobRunnerAction(jobRunnerActionName);

                                // Send update Request
                                updateJobDetails(documentId, updateJobDetails, listener);
                            }, listener::onFailure));
                        } else {
                            // Create JobDetails from params
                            JobDetails tempJobDetails = new JobDetails(
                                jobIndexName,
                                jobTypeName,
                                jobParameterActionName,
                                jobRunnerActionName,
                                extensionUniqueId
                            );

                            // Index new Job Details entry
                            logger.info(
                                "Creating job details for extension unique id " + extensionUniqueId + " : " + tempJobDetails.toString()
                            );
                            createJobDetails(tempJobDetails, listener);
                        }
                    } catch (VersionConflictEngineException e) {
                        logger.debug("could not process job index for extensionUniqueId " + extensionUniqueId, e.getMessage());
                        listener.onResponse(null);
                    }
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }
    }

    /**
     * Create Job details entry
     * @param tempJobDetails new job details object that need to be inserted as document in the index=
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was created
     *                 or else null.
     */
    private void createJobDetails(final JobDetails tempJobDetails, ActionListener<String> listener) {
        try {
            // Create index request, document Id will be randomly generated
            final IndexRequest request = new IndexRequest(JOB_DETAILS_INDEX_NAME).source(
                tempJobDetails.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            ).setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO).setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM).create(true);

            // Index Job Details
            client.index(request, ActionListener.wrap(response -> { listener.onResponse(response.getId()); }, exception -> {
                if (exception instanceof IOException) {
                    logger.error("IOException occurred creating job details", exception);
                }
                listener.onResponse(null);
            }));
        } catch (IOException e) {
            logger.error("IOException occurred creating job details", e);
            listener.onResponse(null);
        }
    }

    /**
     * Find Job details for a particular document Id
     * @param documentId unique id for Job Details document
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was found
     *                 or else null.
     */
    private void findJobDetails(final String documentId, ActionListener<JobDetails> listener) {
        GetRequest getRequest = new GetRequest(JOB_DETAILS_INDEX_NAME).id(documentId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onResponse(null);
            } else {
                try {
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                    parser.nextToken();
                    listener.onResponse(JobDetails.parse(parser));
                } catch (IOException e) {
                    logger.error("IOException occurred finding JobDetails for documentId " + documentId, e);
                    listener.onResponse(null);
                }
            }
        }, exception -> {
            logger.error("Exception occurred finding job details for documentId " + documentId, exception);
            listener.onFailure(exception);
        }));
    }

    /**
     * Delete job details to a corresponding document Id
     * @param documentId unique id to find and delete the job details document in the index
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was deleted
     *                 or else null.
     */
    public void deleteJobDetails(final String documentId, ActionListener<Boolean> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(JOB_DETAILS_INDEX_NAME).id(documentId);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            listener.onResponse(
                response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND
            );
        }, exception -> {
            if (exception instanceof IndexNotFoundException || exception.getCause() instanceof IndexNotFoundException) {
                logger.debug("Index is not found to delete job details for document id. {} " + documentId, exception.getMessage());
                listener.onResponse(true);
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    /**
     * Update Job details to a corresponding documentId
     * @param updateJobDetails update job details object entry
     * @param documentId unique id to find and update the corresponding document mapped to it
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was updated
     *                 or else null.
     */
    private void updateJobDetails(final String documentId, final JobDetails updateJobDetails, ActionListener<String> listener) {
        try {
            UpdateRequest updateRequest = new UpdateRequest().index(JOB_DETAILS_INDEX_NAME)
                .id(documentId)
                .doc(updateJobDetails.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            client.update(updateRequest, ActionListener.wrap(response -> listener.onResponse(response.getId()), exception -> {
                if (exception instanceof VersionConflictEngineException) {
                    logger.debug("could not update job details for documentId " + documentId, exception.getMessage());
                }
                if (exception instanceof DocumentMissingException) {
                    logger.debug("Document is deleted. This happens if the job details is already removed {}", exception.getMessage());
                }
                if (exception instanceof IOException) {
                    logger.error("IOException occurred in updating job details.", exception);
                }
                listener.onResponse(null);
            }));
        } catch (IOException e) {
            logger.error("IOException occurred updating job details for documentId " + documentId, e);
            listener.onResponse(null);
        }
    }

    private String jobDetailsMapping() {
        try {
            InputStream in = JobDetailsService.class.getResourceAsStream(PLUGINS_JOB_DETAILS_MAPPING_FILE);
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null;) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("JobDetails Mapping cannot be read correctly.");
        }
    }

    private static class IndexToJobDetails {
        private static final ConcurrentMap<String, JobDetails> indexToJobDetails = new ConcurrentHashMap<>();

        public static ConcurrentMap<String, JobDetails> getInstance() {
            return IndexToJobDetails.indexToJobDetails;
        }
    }
}
