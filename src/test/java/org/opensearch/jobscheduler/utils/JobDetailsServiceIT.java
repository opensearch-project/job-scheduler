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

    @Before
    public void setup() {
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        this.indicesToListen = new HashSet<>();
        Mockito.when(this.clusterService.state().routingTable().hasIndex(JobDetailsService.JOB_DETAILS_INDEX_NAME))
            .thenReturn(false)
            .thenReturn(true);
    }

    public void testSanity() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);

        jobDetailsService.processJobDetailsForExtensionUniqueId(
            "sample-job-index",
            null,
            "sample-job-parameter",
            "sample-job-runner",
            "sample-extension",
            JobDetailsService.JobDetailsRequestType.JOB_INDEX,
            ActionListener.wrap(jobDetails -> {
                assertNotNull("Expected to successfully get job details.", jobDetails);
                assertEquals("sample-job-index", jobDetails.getJobIndex());
                assertEquals("sample-job-parameter", jobDetails.getJobParameterAction());
                assertEquals("sample-job-runner", jobDetails.getJobRunnerAction());
                assertNull(jobDetails.getJobType());
                jobDetailsService.createJobDetailsIndex(ActionListener.wrap(response -> {
                    assertTrue(response);
                    inProgressFuture.complete(response);
                }, exception -> { fail(exception.getMessage()); }));

                jobDetailsService.deleteJobDetailsForExtension("sample-extension", ActionListener.wrap(response -> {
                    assertTrue(response);
                    inProgressFuture.complete(response);
                }, exception -> { fail(exception.getMessage()); }));
            }, exception -> { fail(exception.getMessage()); })
        );
        inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
    }

    public void testSecondProcessofJobIndexPass() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);

        jobDetailsService.processJobDetailsForExtensionUniqueId(
            "sample-job-index",
            null,
            "sample-job-parameter",
            "sample-job-runner",
            "sample-extension",
            JobDetailsService.JobDetailsRequestType.JOB_INDEX,
            ActionListener.wrap(jobDetails -> {
                jobDetailsService.processJobDetailsForExtensionUniqueId(
                    "sample-job-index1",
                    null,
                    "sample-job-parameter",
                    "sample-job-runner",
                    "sample-extension",
                    JobDetailsService.JobDetailsRequestType.JOB_INDEX,
                    ActionListener.wrap(jobDetails1 -> {
                        assertNotNull("Expected to failed to get get job details for 2nd request.", jobDetails1);
                        assertNotNull("Expected to successfully get job details.", jobDetails);
                        assertEquals("sample-job-index", jobDetails.getJobIndex());
                        assertEquals("sample-job-parameter", jobDetails.getJobParameterAction());
                        assertEquals("sample-job-runner", jobDetails.getJobRunnerAction());
                        assertEquals("sample-job-index1", jobDetails1.getJobIndex());
                        assertNull(jobDetails.getJobType());
                        jobDetailsService.createJobDetailsIndex(ActionListener.wrap(response -> {
                            assertTrue(response);
                            inProgressFuture.complete(response);
                        }, exception -> { fail(exception.getMessage()); }));

                        jobDetailsService.deleteJobDetailsForExtension("sample-extension", ActionListener.wrap(response -> {
                            assertTrue(response);
                            inProgressFuture.complete(response);
                        }, exception -> { fail(exception.getMessage()); }));
                    }, exception -> { fail(exception.getMessage()); })
                );
            },
                exception -> { fail(exception.getMessage()); }

            )
        );

        inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
    }

    public void testDeleteJobDetailsWithOutExtensionIdCreation() throws ExecutionException, InterruptedException, TimeoutException {
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);
        jobDetailsService.deleteJobDetailsForExtension(
            "demo-extension",
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
                jobDetailsService.deleteJobDetailsForExtension(
                    "demo-extension",
                    ActionListener.wrap(
                        deleted -> { assertTrue("Failed to delete job details for extension.", deleted); },
                        exception -> fail(exception.getMessage())
                    )
                );
            } else {
                fail("Failed to job details for extension");
            }

        }, exception -> fail(exception.getMessage())));
    }

    public void testUpdateIndexToJobDetails() throws ExecutionException, InterruptedException, TimeoutException {

        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService, this.indicesToListen);

        String expectedExtensionUniqueId = "sample-extension";
        String expectedJobIndex = "sample-job-index";
        String expectedJobType = "sample-job-type";
        String expectedJobParamAction = "sample-job-parameter";
        String expectedJobRunnerAction = "sample-job-runner";

        // Index Job Index name, actions, for extensionUniqueID
        jobDetailsService.processJobDetailsForExtensionUniqueId(
            expectedJobIndex,
            null,
            expectedJobParamAction,
            expectedJobRunnerAction,
            expectedExtensionUniqueId,
            JobDetailsService.JobDetailsRequestType.JOB_INDEX,
            ActionListener.wrap(jobDetailsWithoutJobType -> {
                // Index Job Type
                jobDetailsService.processJobDetailsForExtensionUniqueId(
                    null,
                    expectedJobType,
                    null,
                    null,
                    expectedExtensionUniqueId,
                    JobDetailsService.JobDetailsRequestType.JOB_TYPE,
                    ActionListener.wrap(jobDetails -> {

                        // Ensure job details entry is valid
                        assertEquals(expectedJobIndex, jobDetails.getJobIndex());
                        assertEquals(expectedJobType, jobDetails.getJobType());
                        assertEquals(expectedJobParamAction, jobDetails.getJobParameterAction());
                        assertEquals(expectedJobRunnerAction, jobDetails.getJobRunnerAction());

                        // We'll have to invoke updateIndexToJobDetails as jobDetailsService is added as an indexOperationListener
                        // onIndexModule
                        jobDetailsService.updateIndexToJobDetails(expectedExtensionUniqueId, jobDetails);

                        // Ensure indicesToListen is updated
                        assertTrue(this.indicesToListen.contains(jobDetails.getJobIndex()));

                        // Ensure indexToJobDetails is updated
                        JobDetails entry = jobDetailsService.getIndexToJobDetails().get(expectedExtensionUniqueId);
                        assertEquals(expectedJobIndex, entry.getJobIndex());
                        assertEquals(expectedJobType, entry.getJobType());
                        assertEquals(expectedJobParamAction, entry.getJobParameterAction());
                        assertEquals(expectedJobRunnerAction, entry.getJobRunnerAction());

                        inProgressFuture.complete(true);

                    }, exception -> { inProgressFuture.completeExceptionally(exception); })
                );
            }, exception -> { inProgressFuture.completeExceptionally(exception); })
        );

        try {
            inProgressFuture.get(JobDetailsService.TIME_OUT_FOR_REQUEST, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

}
