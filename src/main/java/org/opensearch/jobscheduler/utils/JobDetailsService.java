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
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.jobscheduler.model.JobDetails;

import java.nio.charset.StandardCharsets;

public final class JobDetailsService {

    private static final Logger logger = LogManager.getLogger(JobDetailsService.class);
    private static final String JOB_DETAILS_INDEX_NAME = ".opensearch-plugins-job-details";

    private final Client client;
    private final ClusterService clusterService;

    public JobDetailsService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public boolean jobDetailsIndexExist() {
        return clusterService.state().routingTable().hasIndex(JOB_DETAILS_INDEX_NAME);
    }

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

    public void processJobIndexForExtensionId(
        final String jobIndexName,
        final String jobTypeName,
        final String jobParserActionName,
        final String jobRunnerActionName,
        final String extensionId,
        final JobDetailsRequestType requestType,
        ActionListener<JobDetails> listener
    ) {
        logger.info("Processing request");
        boolean isJobIndexRequest;
        if (requestType.JOB_INDEX == requestType) {
            isJobIndexRequest = true;
            if (jobIndexName == null) {
                listener.onFailure(new IllegalArgumentException("Job Index Name should not be null"));
            } else if (jobParserActionName == null) {
                listener.onFailure(new IllegalArgumentException("Job Parser Action Name should not be null"));
            } else if (jobRunnerActionName == null) {
                listener.onFailure(new IllegalArgumentException("Job Runner Action Name should not be null"));
            }
        } else {
            isJobIndexRequest = false;
            if (jobTypeName == null) {
                listener.onFailure(new IllegalArgumentException("Job Type Name should not be null"));
            }
        }
        if (extensionId == null) {
            listener.onFailure(new IllegalArgumentException("Extension Id should not be null"));
        } else {
            createJobDetailsIndex(ActionListener.wrap(created -> {
                if (created) {
                    try {
                        logger.info("Processing get request now");

                        findJobDetailsForExtensionId(extensionId, ActionListener.wrap(existingJobDetails -> {
                            if (existingJobDetails != null) {
                                logger.debug("Updating job details for extension id: " + extensionId + existingJobDetails);
                                JobDetails updateJobDetails = new JobDetails(existingJobDetails);
                                logger.info("Processing update request now");
                                if (isJobIndexRequest) {
                                    updateJobDetails.setJobIndex(jobIndexName);
                                    updateJobDetails.setJobParserAction(jobParserActionName);
                                    updateJobDetails.setJobRunnerAction(jobRunnerActionName);
                                } else {
                                    updateJobDetails.setJobType(jobTypeName);
                                }
                                updateJobDetailsForExtensionId(updateJobDetails, extensionId, listener);

                            } else {
                                logger.info("Processing create request now");
                                JobDetails tempJobDetails = new JobDetails();
                                if (isJobIndexRequest) {
                                    tempJobDetails.setJobIndex(jobIndexName);
                                    tempJobDetails.setJobParserAction(jobParserActionName);
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

    private void createJobDetailsForExtensionId(final JobDetails tempJobDetails, String extensionId, ActionListener<JobDetails> listener) {
        logger.info("Processing create Job details method now");
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

    private void findJobDetailsForExtensionId(final String extensionId, ActionListener<JobDetails> listener) {
        logger.info("Processing find Job details method now");
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

    private void updateJobDetailsForExtensionId(
        final JobDetails updateJobDetails,
        final String extensionId,
        ActionListener<JobDetails> listener
    ) {
        logger.info("Processing update Job details method now");
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
            InputStream in = JobDetailsService.class.getResourceAsStream("/mappings/opensearch_plugins_job_details.json");
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null;) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("JobDetails Mapping cannot be read correctly.");
        }
        // String response = null;
        // try {

        // URL url = JobDetailsService.class.getClassLoader().getResource("mappings/opensearch_plugins_job_details.json");
        // response = Resources.toString(url, Charsets.UTF_8);
        // } catch (IOException e) {
        // logger.info("getJobMapping failed", e);
        // }

        // return response;
    }

    public enum JobDetailsRequestType {
        JOB_INDEX,
        JOB_TYPE
    }
}
