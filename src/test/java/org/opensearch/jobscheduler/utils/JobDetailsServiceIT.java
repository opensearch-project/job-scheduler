/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.ExtensionJobActionRequest;
import org.opensearch.jobscheduler.transport.ExtensionJobActionResponse;
import org.opensearch.jobscheduler.transport.JobParameterRequest;
import org.opensearch.jobscheduler.transport.JobParameterResponse;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.JobRunnerResponse;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.jobscheduler.ScheduledJobProvider;

public class JobDetailsServiceIT extends OpenSearchIntegTestCase {

    private ClusterService clusterService;
    private Set<String> indicesToListen;
    private Map<String, ScheduledJobProvider> indexToJobProviders;

    private String expectedJobIndex;
    private String expectedJobType;
    private String expectedJobParamAction;
    private String expectedJobRunnerAction;
    private String expectedExtensionUniqueId;

    private String expectedDocumentId;
    private String updatedJobIndex;

    private ExtensionJobParameter extensionJobParameter;

    @Before
    public void setup() {
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex(JobDetailsService.JOB_DETAILS_INDEX_NAME))
            .thenReturn(false)
            .thenReturn(true);

        this.indicesToListen = new HashSet<>();
        this.indexToJobProviders = new HashMap<>();

        this.expectedJobIndex = "sample-job-index";
        this.expectedJobType = "sample-job-type";
        this.expectedJobParamAction = "sample-job-parameter";
        this.expectedJobRunnerAction = "sample-job-runner";
        this.expectedExtensionUniqueId = "sample-extension";

        this.expectedDocumentId = "sample-document-id";
        this.updatedJobIndex = "updated-job-index";

        this.extensionJobParameter = new ExtensionJobParameter(
            "jobName",
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            Instant.now(),
            Instant.now(),
            true,
            2L,
            2.0
        );
    }

    public void testGetJobDetailsSanity() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );

        jobDetailsService.processJobDetails(
            null,
            expectedJobIndex,
            expectedJobType,
            expectedJobParamAction,
            expectedJobRunnerAction,
            expectedExtensionUniqueId,
            ActionListener.wrap(indexedDocumentId -> {

                // Ensure that indexedDocumentId is nbt null
                assertNotNull(indexedDocumentId);

                jobDetailsService.createJobDetailsIndex(ActionListener.wrap(response -> {
                    assertTrue(response);
                    inProgressFuture.complete(response);
                }, exception -> { fail(exception.getMessage()); }));

                jobDetailsService.deleteJobDetails(this.expectedDocumentId, ActionListener.wrap(response -> {
                    assertTrue(response);
                    inProgressFuture.complete(response);
                }, exception -> { fail(exception.getMessage()); }));
            }, exception -> { fail(exception.getMessage()); })
        );

        inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
    }

    public void testUpdateJobDetailsSanity() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<String> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );

        // Create initial index request
        jobDetailsService.processJobDetails(
            null,
            expectedJobIndex,
            expectedJobType,
            expectedJobParamAction,
            expectedJobRunnerAction,
            expectedExtensionUniqueId,
            ActionListener.wrap(indexedDocumentId -> {
                assertNotNull(indexedDocumentId);

                // submit update request to change the job index name for the same document Id
                jobDetailsService.processJobDetails(
                    indexedDocumentId,
                    updatedJobIndex,
                    expectedJobType,
                    expectedJobParamAction,
                    expectedJobRunnerAction,
                    expectedExtensionUniqueId,
                    ActionListener.wrap(updatedIndexedDocumentId -> {

                        // Ensure that the response document ID matches the initial document ID
                        assertNotNull(updatedIndexedDocumentId);
                        assertEquals(indexedDocumentId, updatedIndexedDocumentId);
                        inProgressFuture.complete(updatedIndexedDocumentId);

                    }, exception -> fail(exception.getMessage()))
                );
            }, exception -> fail(exception.getMessage()))
        );

        inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
    }

    public void testDeleteJobDetailsWithOutDocumentIdCreation() throws ExecutionException, InterruptedException, TimeoutException {
        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );
        jobDetailsService.deleteJobDetails(
            expectedDocumentId,
            ActionListener.wrap(
                deleted -> { assertTrue("Failed to delete JobDetails.", deleted); },
                exception -> { fail(exception.getMessage()); }
            )
        );
    }

    public void testDeleteNonExistingJobDetails() throws ExecutionException, InterruptedException, TimeoutException {
        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );
        jobDetailsService.createJobDetailsIndex(ActionListener.wrap(created -> {
            if (created) {
                jobDetailsService.deleteJobDetails(
                    expectedDocumentId,
                    ActionListener.wrap(
                        deleted -> { assertTrue("Failed to delete job details for documentId.", deleted); },
                        exception -> fail(exception.getMessage())
                    )
                );
            } else {
                fail("Failed to job details for extension");
            }

        }, exception -> fail(exception.getMessage())));
    }

    public void testUpdateIndexToJobDetails() throws ExecutionException, InterruptedException, TimeoutException {

        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );
        JobDetails jobDetails = new JobDetails(
            expectedJobIndex,
            expectedJobType,
            expectedJobParamAction,
            expectedJobRunnerAction,
            expectedExtensionUniqueId
        );

        // We'll have to invoke updateIndexToJobDetails as jobDetailsService is added as an indexOperationListener
        // onIndexModule
        jobDetailsService.updateIndexToJobDetails(expectedDocumentId, jobDetails);

        // Ensure indicesToListen is updated
        assertTrue(this.indicesToListen.contains(jobDetails.getJobIndex()));

        // Ensure indexToJobDetails is updated
        JobDetails entry = jobDetailsService.getIndexToJobDetails().get(expectedDocumentId);
        assertEquals(expectedJobIndex, entry.getJobIndex());
        assertEquals(expectedJobType, entry.getJobType());
        assertEquals(expectedJobParamAction, entry.getJobParameterAction());
        assertEquals(expectedJobRunnerAction, entry.getJobRunnerAction());
        assertEquals(expectedExtensionUniqueId, entry.getExtensionUniqueId());

    }

    public void testUpdateIndexToJobProviders() {
        JobDetailsService jobDetailsService = new JobDetailsService(
            client(),
            this.clusterService,
            this.indicesToListen,
            this.indexToJobProviders
        );
        JobDetails jobDetails = new JobDetails(
            expectedJobIndex,
            expectedJobType,
            expectedJobParamAction,
            expectedJobRunnerAction,
            expectedExtensionUniqueId
        );

        // Create job provider for given job details entry
        jobDetailsService.updateIndexToJobProviders(jobDetails);

        // Ensure that the indexToJobProviders is updated
        ScheduledJobProvider provider = jobDetailsService.getIndexToJobProviders().get(jobDetails.getJobIndex());
        assertEquals(expectedJobIndex, provider.getJobIndexName());
        assertEquals(expectedJobType, provider.getJobType());
        assertNotNull(provider.getJobParser());
        assertNotNull(provider.getJobRunner());
    }

    private void compareExtensionJobParameters(
        ExtensionJobParameter extensionJobParameter,
        ExtensionJobParameter deserializedJobParameter
    ) {
        assertEquals(extensionJobParameter.getName(), deserializedJobParameter.getName());
        assertEquals(extensionJobParameter.getSchedule(), deserializedJobParameter.getSchedule());
        assertEquals(extensionJobParameter.getLastUpdateTime(), deserializedJobParameter.getLastUpdateTime());
        assertEquals(extensionJobParameter.getEnabledTime(), deserializedJobParameter.getEnabledTime());
        assertEquals(extensionJobParameter.isEnabled(), deserializedJobParameter.isEnabled());
        assertEquals(extensionJobParameter.getLockDurationSeconds(), deserializedJobParameter.getLockDurationSeconds());
        assertEquals(extensionJobParameter.getJitter(), deserializedJobParameter.getJitter());
    }

    public void testJobRunnerExtensionJobActionRequest() throws IOException {

        LockService lockService = new LockService(client(), this.clusterService);
        JobExecutionContext jobExecutionContext = new JobExecutionContext(
            Instant.now(),
            new JobDocVersion(0, 0, 0),
            lockService,
            "indexName",
            "id"
        );

        // Create JobRunner Request
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest("placeholder", this.extensionJobParameter, jobExecutionContext);
        ExtensionActionRequest actionRequest = new ExtensionJobActionRequest<JobRunnerRequest>("actionName", jobRunnerRequest);

        // Test ExtensionActionRequest deserialization
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {

                actionRequest = new ExtensionActionRequest(in);

                // Test deserialization of action request params
                JobRunnerRequest deserializedRequest = new JobRunnerRequest(actionRequest.getRequestBytes());

                // Test deserialization of extension job parameter
                ExtensionJobParameter deserializedJobParameter = deserializedRequest.getJobParameter();
                compareExtensionJobParameters(this.extensionJobParameter, deserializedJobParameter);

                // Test deserialization of job execution context
                JobExecutionContext deserializedJobExecutionContext = deserializedRequest.getJobExecutionContext();
                assertEquals(jobExecutionContext.getJobId(), deserializedJobExecutionContext.getJobId());
                assertEquals(jobExecutionContext.getJobIndexName(), deserializedJobExecutionContext.getJobIndexName());
                assertEquals(jobExecutionContext.getExpectedExecutionTime(), deserializedJobExecutionContext.getExpectedExecutionTime());
                assertEquals(0, jobExecutionContext.getJobVersion().compareTo(deserializedJobExecutionContext.getJobVersion()));
            }
        }
    }

    public void testJobParameterExtensionJobActionRequest() throws IOException {

        String content = "{\"test_field\":\"test\"}";
        JobDocVersion jobDocVersion = new JobDocVersion(1L, 1L, 1L);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content.getBytes());

        // Create JobParameterRequest
        JobParameterRequest jobParamRequest = new JobParameterRequest("placeholder", parser, "id", jobDocVersion);
        ExtensionActionRequest actionRequest = new ExtensionJobActionRequest<JobParameterRequest>("actionName", jobParamRequest);

        // Test ExtensionActionRequest deserialization
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                actionRequest = new ExtensionActionRequest(in);

                // Test deserialization of action request params
                JobParameterRequest deserializedRequest = new JobParameterRequest(actionRequest.getRequestBytes());
                assertEquals(jobParamRequest.getId(), deserializedRequest.getId());
                assertEquals(jobParamRequest.getJobSource(), deserializedRequest.getJobSource());

                // Test deserialization of job doc version
                assertEquals(0, jobParamRequest.getJobDocVersion().compareTo(deserializedRequest.getJobDocVersion()));
            }
        }
    }

    public void testJobRunnerExtensionJobActionResponse() throws IOException {

        // Create JobRunnerResponse
        JobRunnerResponse jobRunnerResponse = new JobRunnerResponse(true);
        ExtensionActionResponse actionResponse = new ExtensionJobActionResponse<JobRunnerResponse>(jobRunnerResponse);

        // Test ExtensionActionResponse deserialization
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {

                actionResponse = new ExtensionActionResponse(in);

                // Test deserialization of action response params
                JobRunnerResponse deserializedResponse = new JobRunnerResponse(actionResponse.getResponseBytes());
                assertEquals(jobRunnerResponse.getJobRunnerStatus(), deserializedResponse.getJobRunnerStatus());
            }
        }

    }

    public void testJobParameterExtensionJobActionResponse() throws IOException {

        // Create JobParameterResponse
        JobParameterResponse jobParameterResponse = new JobParameterResponse(this.extensionJobParameter);
        ExtensionActionResponse actionResponse = new ExtensionJobActionResponse<JobParameterResponse>(jobParameterResponse);

        // Test ExtensionActionReseponse deserialization
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {

                actionResponse = new ExtensionActionResponse(in);

                // Test deserialization of action response params
                JobParameterResponse deserializedResponse = new JobParameterResponse(actionResponse.getResponseBytes());
                compareExtensionJobParameters(this.extensionJobParameter, deserializedResponse.getJobParameter());
            }
        }
    }

}
