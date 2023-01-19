/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.model.JobDetails;
import org.opensearch.test.OpenSearchIntegTestCase;

public class JobDetailsServiceIT extends OpenSearchIntegTestCase {

    private ClusterService clusterService;
    private Set<String> indicesToListen;

    private String expectedJobIndex;
    private String expectedJobType;
    private String expectedJobParamAction;
    private String expectedJobRunnerAction;
    private String expectedExtensionUniqueId;

    private String expectedDocumentId;
    private String updatedJobIndex;

    @Before
    public void setup() {
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex(JobDetailsService.JOB_DETAILS_INDEX_NAME))
            .thenReturn(false)
            .thenReturn(true);

        this.indicesToListen = new HashSet<>();

        this.expectedJobIndex = "sample-job-index";
        this.expectedJobType = "sample-job-type";
        this.expectedJobParamAction = "sample-job-parameter";
        this.expectedJobRunnerAction = "sample-job-runner";
        this.expectedExtensionUniqueId = "sample-extension";

        this.expectedDocumentId = "sample-document-id";
        this.updatedJobIndex = "updated-job-index";
    }

    public void testGetJobDetailsSanity() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);

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
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);

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
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);
        jobDetailsService.deleteJobDetails(
            expectedDocumentId,
            ActionListener.wrap(
                deleted -> { assertTrue("Failed to delete JobDetails.", deleted); },
                exception -> { fail(exception.getMessage()); }
            )
        );
    }

    public void testDeleteNonExistingJobDetails() throws ExecutionException, InterruptedException, TimeoutException {
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);
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

        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);
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

}
