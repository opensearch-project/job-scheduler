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
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.jobscheduler.model.JobDetails;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JobDetailsService implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(JobDetailsService.class);
    public static final String JOB_DETAILS_INDEX_NAME = ".opensearch-plugins-job-details";
    private static final String PLUGINS_JOB_DETAILS_MAPPING_FILE = "/mappings/opensearch_plugins_job_details.json";

    public static Long TIME_OUT_FOR_REQUEST = 10L;
    private final Client client;
    private final ClusterService clusterService;
    private Set<String> indicesToListen;

    private static final ConcurrentMap<String, JobDetails> indexToJobDetails = IndexToJobDetails.getInstance();

    public JobDetailsService(final Client client, final ClusterService clusterService, Set<String> indicesToListen) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesToListen = indicesToListen;
    }

    public boolean jobDetailsIndexExist() {
        return clusterService.state().routingTable().hasIndex(JOB_DETAILS_INDEX_NAME);
    }

    public static ConcurrentMap<String, JobDetails> getIndexToJobDetails() {
        return JobDetailsService.indexToJobDetails;
    }

    private void updateIndicesToListen(String jobIndex) {
        this.indicesToListen.add(jobIndex);
    }

    /**
     * Registers or updates a jobDetails entry
     *
     * @param extensionId the unique Id for the job details
     * @param jobDetails the jobDetails to register
     */
    void updateIndexToJobDetails(String extensionId, JobDetails jobDetails) {
        if (indexToJobDetails.containsKey(extensionId)) {
            if (jobDetails.getJobType() != null) {
                // Update JobDetails entry with job type
                JobDetails existingJobDetails = indexToJobDetails.get(extensionId);
                existingJobDetails.setJobType(jobDetails.getJobType());
            }
        } else {
            // Register JobDetails entry
            indexToJobDetails.put(extensionId, jobDetails);
            updateIndicesToListen(jobDetails.getJobIndex());
        }
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
     * Attempts to process job details with a specific extension Id. If the job details does not exist it attempts to create the job details document.
     * If the job details document exists, it will try to update the job details.
     *
     * @param jobIndexName a non-null job index name.
     * @param jobTypeName a non-null job type name.
     * @param jobParameterActionName a non-null job parameter action name.
     * @param jobRunnerActionName a non-null job runner action name.
     * @param extensionId the unique Id for the job details.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was processed
     *                 or else null.
     */
    public void processJobDetailsForExtensionId(
        final String jobIndexName,
        final String jobTypeName,
        final String jobParameterActionName,
        final String jobRunnerActionName,
        final String extensionId,
        final JobDetailsRequestType requestType,
        ActionListener<JobDetails> listener
    ) {
        boolean isJobIndexRequest;
        if (requestType.JOB_INDEX == requestType) {
            isJobIndexRequest = true;
            if (jobIndexName == null
                || jobIndexName.isEmpty()
                || jobParameterActionName == null
                || jobParameterActionName.isEmpty()
                || jobRunnerActionName == null
                || jobRunnerActionName.isEmpty()) {
                listener.onFailure(
                    new IllegalArgumentException("JobIndexName, JobParameterActionName, JobRunnerActionName must not be null or empty")
                );
            }
        } else {
            isJobIndexRequest = false;
            if (jobTypeName == null || jobTypeName.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("Job Type Name must not be null or empty"));
            }
        }
        if (extensionId == null || extensionId.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("Extension Id must not be null or empty"));
        } else {
            createJobDetailsIndex(ActionListener.wrap(created -> {
                if (created) {
                    try {
                        findJobDetailsForExtensionId(extensionId, ActionListener.wrap(existingJobDetails -> {
                            if (existingJobDetails != null) {
                                logger.debug("Updating job details for extension id: " + extensionId + existingJobDetails);
                                JobDetails updateJobDetails = new JobDetails(existingJobDetails);
                                if (isJobIndexRequest) {
                                    updateJobDetails.setJobIndex(jobIndexName);
                                    updateJobDetails.setJobParameterAction(jobParameterActionName);
                                    updateJobDetails.setJobRunnerAction(jobRunnerActionName);
                                } else {
                                    updateJobDetails.setJobType(jobTypeName);
                                }
                                updateJobDetailsForExtensionId(updateJobDetails, extensionId, listener);

                            } else {
                                JobDetails tempJobDetails = new JobDetails();
                                if (isJobIndexRequest) {
                                    tempJobDetails.setJobIndex(jobIndexName);
                                    tempJobDetails.setJobParameterAction(jobParameterActionName);
                                    tempJobDetails.setJobRunnerAction(jobRunnerActionName);
                                } else {
                                    tempJobDetails.setJobType(jobTypeName);
                                }
                                logger.debug(
                                    "Job Details for extension Id "
                                        + extensionId
                                        + " does not exist. Creating new Job Details"
                                        + tempJobDetails
                                );
                                createJobDetailsForExtensionId(tempJobDetails, extensionId, listener);
                            }
                        }, listener::onFailure));
                    } catch (VersionConflictEngineException e) {
                        logger.debug("could not process job index for extensionId " + extensionId, e.getMessage());
                        listener.onResponse(null);
                    }
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        }
    }

    /**
     * Create Job details entry for extension id
     * @param tempJobDetails new job details object that need to be inserted as document in the index
     * @param extensionId  unique id to create the entry for job details
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was created
     *                 or else null.
     */
    private void createJobDetailsForExtensionId(final JobDetails tempJobDetails, String extensionId, ActionListener<JobDetails> listener) {
        try {
            final IndexRequest request = new IndexRequest(JOB_DETAILS_INDEX_NAME).id(extensionId)
                .source(tempJobDetails.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                .setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                .create(true);
            client.index(request, ActionListener.wrap(response -> listener.onResponse(new JobDetails(tempJobDetails)), exception -> {
                if (exception instanceof VersionConflictEngineException) {
                    logger.debug("Job Details for extension id " + extensionId + " is already created. {}", exception.getMessage());
                }
                if (exception instanceof IOException) {
                    logger.error("IOException occurred creating job details", exception);
                }
                listener.onResponse(null);
            }));
        } catch (IOException e) {
            logger.error("IOException occurred creating job details for extension id " + extensionId, e);
            listener.onResponse(null);
        }
    }

    /**
     * Find extension corresponding to an extension id
     * @param extensionId unique id to find the job details document in the index
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was found
     *                 or else null.
     */
    private void findJobDetailsForExtensionId(final String extensionId, ActionListener<JobDetails> listener) {
        GetRequest getRequest = new GetRequest(JOB_DETAILS_INDEX_NAME).id(extensionId);
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
                    logger.error("IOException occurred finding JobDetails for extension id " + extensionId, e);
                    listener.onResponse(null);
                }
            }
        }, exception -> {
            logger.error("Exception occurred finding job details for extension id " + extensionId, exception);
            listener.onFailure(exception);
        }));
    }

    /**
     * Delete job details to a corresponding extension id
     * @param extensionId unique id to find and delete the job details document in the index
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was deleted
     *                 or else null.
     */
    public void deleteJobDetailsForExtension(final String extensionId, ActionListener<Boolean> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(JOB_DETAILS_INDEX_NAME).id(extensionId);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            listener.onResponse(
                response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND
            );
        }, exception -> {
            if (exception instanceof IndexNotFoundException || exception.getCause() instanceof IndexNotFoundException) {
                logger.debug("Index is not found to delete job details for extension id. {} " + extensionId, exception.getMessage());
                listener.onResponse(true);
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    /**
     * Update Job details to a corresponding extension Id
     * @param updateJobDetails update job details object entry
     * @param extensionId unique id to find and update the corresponding document mapped to it
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the job details if it was updated
     *                 or else null.
     */
    private void updateJobDetailsForExtensionId(
        final JobDetails updateJobDetails,
        final String extensionId,
        ActionListener<JobDetails> listener
    ) {
        try {
            UpdateRequest updateRequest = new UpdateRequest().index(JOB_DETAILS_INDEX_NAME)
                .id(extensionId)
                .doc(updateJobDetails.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .fetchSource(true);

            client.update(
                updateRequest,
                ActionListener.wrap(response -> listener.onResponse(new JobDetails(updateJobDetails)), exception -> {
                    if (exception instanceof VersionConflictEngineException) {
                        logger.debug("could not update job details for extensionId " + extensionId, exception.getMessage());
                    }
                    if (exception instanceof DocumentMissingException) {
                        logger.debug("Document is deleted. This happens if the job details is already removed {}", exception.getMessage());
                    }
                    if (exception instanceof IOException) {
                        logger.error("IOException occurred in updating job details.", exception);
                    }
                    listener.onResponse(null);
                })
            );
        } catch (IOException e) {
            logger.error("IOException occurred updating job details for extension id " + extensionId, e);
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

    public enum JobDetailsRequestType {
        JOB_INDEX,
        JOB_TYPE
    }

    private static class IndexToJobDetails {
        private static final ConcurrentMap<String, JobDetails> indexToJobDetails = new ConcurrentHashMap<>();

        public static ConcurrentMap<String, JobDetails> getInstance() {
            return IndexToJobDetails.indexToJobDetails;
        }
    }
}
