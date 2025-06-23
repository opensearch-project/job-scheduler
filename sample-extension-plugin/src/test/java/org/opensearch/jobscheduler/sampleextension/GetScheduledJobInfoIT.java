/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.junit.Before;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class GetScheduledJobInfoIT extends SampleExtensionIntegTestCase {

    @Before
    public void setupJobs() throws IOException, InterruptedException {
        SampleJobParameter jobParam1 = new SampleJobParameter(
            "test-job-1",
            "Test Job 1",
            "test-index-1",
            new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
            30L,
            0.1
        );

        SampleJobParameter jobParam2 = new SampleJobParameter(
            "test-job-2",
            "Test Job 2",
            "test-index-2",
            new IntervalSchedule(Instant.now(), 10, ChronoUnit.MINUTES),
            60L,
            0.2
        );

        SampleJobParameter jobParam3 = new SampleJobParameter(
            "test-job-3",
            "Test Job 3",
            "test-index-3",
            new IntervalSchedule(Instant.now(), 15, ChronoUnit.MINUTES),
            90L,
            0.3
        );

        SampleJobParameter jobParam4 = new SampleJobParameter(
            "test-job-4",
            "Test Job 4",
            "test-index-4",
            new IntervalSchedule(Instant.now(), 20, ChronoUnit.MINUTES),
            120L,
            0.4
        );

        createWatcherJob("test-job-1", jobParam1);
        createWatcherJob("test-job-2", jobParam2);
        createWatcherJob("test-job-3", jobParam3);
        createWatcherJob("test-job-4", jobParam4);
        // Refresh indices to ensure all jobs are available
        makeRequest(client(), "POST", "/_refresh", Collections.emptyMap(), null);

        Thread.sleep(1000);
    }

    public void testGetScheduledJobInfoEntireCluster() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/info", Collections.emptyMap(), null);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseJson);
        assertTrue("Response should contain scheduled job information", responseJson.containsKey("jobs"));
    }

    public void testGetScheduledJobInfoByNode() throws IOException {

        Response response = makeRequest(client(), "GET", "/_plugins/_job_scheduler/info?by_node=true", Collections.emptyMap(), null);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseJson);
        assertTrue("Response should contain scheduled job information", responseJson.containsKey("nodes"));
    }

    @After
    public void cleanupJobs() throws IOException {
        deleteWatcherJob("test-job-1");
        deleteWatcherJob("test-job-2");
        deleteWatcherJob("test-job-3");
        deleteWatcherJob("test-job-4");
    }
}
