/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchIntegTestCase;

public class JobDetailsServiceIT extends OpenSearchIntegTestCase {

    private ClusterService clusterService;

    @Before
    public void setup() {
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex("opensearch-plugins-job-details"))
            .thenReturn(false)
            .thenReturn(true);
    }

    public void testSanity() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService);

        jobDetailsService.processJobDetailsForExtensionId(
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
                    latch.countDown();
                }, exception -> { fail(exception.getMessage()); }));

                jobDetailsService.deleteJobDetailsForExtension("sample-extension", ActionListener.wrap(response -> {
                    assertTrue(response);
                    latch.countDown();
                }, exception -> { fail(exception.getMessage()); }));
            }, exception -> { fail(exception.getMessage()); })
        );
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testSecondProcessofJobIndexPass() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService);

        jobDetailsService.processJobDetailsForExtensionId(
            "sample-job-index",
            null,
            "sample-job-parameter",
            "sample-job-runner",
            "sample-extension",
            JobDetailsService.JobDetailsRequestType.JOB_INDEX,
            ActionListener.wrap(jobDetails -> {
                jobDetailsService.processJobDetailsForExtensionId(
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
                            latch.countDown();
                        }, exception -> { fail(exception.getMessage()); }));

                        jobDetailsService.deleteJobDetailsForExtension("sample-extension", ActionListener.wrap(response -> {
                            assertTrue(response);
                            latch.countDown();
                        }, exception -> { fail(exception.getMessage()); }));
                    }, exception -> { fail(exception.getMessage()); })
                );
            },
                exception -> { fail(exception.getMessage()); }

            )
        );

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testDeleteJobDetailsWithOutExtensionIdCreation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService);
        jobDetailsService.deleteJobDetailsForExtension(
            "demo-extension",
            ActionListener.wrap(
                deleted -> { assertTrue("Failed to delete JobDetails.", deleted); },
                exception -> { fail(exception.getMessage()); }
            )
        );
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testDeleteNonExistingJobDetails() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        JobDetailsService jobDetailsService = new JobDetailsService(client(), this.clusterService);
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
        latch.await(5L, TimeUnit.SECONDS);
    }

}
