/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class ScheduledJobProviderTests extends OpenSearchTestCase {

    private static final String JOB_TYPE = "test_job_type";
    private static final String JOB_INDEX_NAME = "test_job_index";

    @Mock
    private ScheduledJobParser jobParser;

    @Mock
    private ScheduledJobRunner jobRunner;

    private ScheduledJobProvider scheduledJobProvider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        scheduledJobProvider = new ScheduledJobProvider(JOB_TYPE, JOB_INDEX_NAME, jobParser, jobRunner);
    }

    public void testGetJobType() {
        assertEquals(JOB_TYPE, scheduledJobProvider.getJobType());
    }

    public void testGetJobIndexName() {
        assertEquals(JOB_INDEX_NAME, scheduledJobProvider.getJobIndexName());
    }

    public void testGetJobParser() {
        assertEquals(jobParser, scheduledJobProvider.getJobParser());
    }

    public void testGetJobRunner() {
        assertEquals(jobRunner, scheduledJobProvider.getJobRunner());
    }

    public void testConstructor() {
        ScheduledJobParser parser = mock(ScheduledJobParser.class);
        ScheduledJobRunner runner = mock(ScheduledJobRunner.class);
        ScheduledJobProvider provider = new ScheduledJobProvider(JOB_TYPE, JOB_INDEX_NAME, parser, runner);
        assertEquals(JOB_TYPE, provider.getJobType());
        assertEquals(JOB_INDEX_NAME, provider.getJobIndexName());
        assertEquals(parser, provider.getJobParser());
        assertEquals(runner, provider.getJobRunner());
    }
}
