/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.model;

import org.junit.Before;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtensionJobParameterTests extends OpenSearchTestCase {

    private String jobName = "testJob";
    Schedule schedule;
    private Instant lastUpdateTime = Instant.ofEpochSecond(1609459200);
    private Instant enabledTime = Instant.ofEpochSecond(1609459200);;
    private boolean isEnabled = true;
    private Long lockDurationSeconds = 60L;
    private Double jitter = 0.1;
    private ExtensionJobParameter extensionJobParameter;

    public ExtensionJobParameterTests() throws IOException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        extensionJobParameter = new ExtensionJobParameter(
            jobName,
            schedule,
            lastUpdateTime,
            enabledTime,
            isEnabled,
            lockDurationSeconds,
            jitter
        );
    }

    public void testConstructorWithCronSchedule() throws IOException {
        String jobName = "testJob";
        Instant lastUpdateTime = Instant.ofEpochSecond(1609459200);
        Instant enabledTime = Instant.ofEpochSecond(1609459200);
        boolean isEnabled = true;
        Long lockDurationSeconds = 60L;
        Double jitter = 0.1;
        assertEquals(jobName, extensionJobParameter.getName());
        assertEquals(schedule, extensionJobParameter.getSchedule());
        assertEquals(lastUpdateTime, extensionJobParameter.getLastUpdateTime());
        assertEquals(enabledTime, extensionJobParameter.getEnabledTime());
        assertTrue(extensionJobParameter.isEnabled());
        assertEquals(lockDurationSeconds, extensionJobParameter.getLockDurationSeconds());
        assertEquals(jitter, extensionJobParameter.getJitter());
        assertEquals(isEnabled, extensionJobParameter.isEnabled());
    }

    public void testExtensionJobParameterFromScheduledJobParameter() {
        String jobName = "test-job";
        Instant lastUpdateTime = Instant.now();
        Instant enabledTime = Instant.now().plusSeconds(3600);
        boolean isEnabled = true;
        Long lockDurationSeconds = 60L;
        Double jitter = 0.5;
        ScheduledJobParameter mockJobParameter = mock(ScheduledJobParameter.class);
        when(mockJobParameter.getName()).thenReturn(jobName);
        when(mockJobParameter.getLastUpdateTime()).thenReturn(lastUpdateTime);
        when(mockJobParameter.getEnabledTime()).thenReturn(enabledTime);
        when(mockJobParameter.isEnabled()).thenReturn(isEnabled);
        when(mockJobParameter.getLockDurationSeconds()).thenReturn(lockDurationSeconds);
        when(mockJobParameter.getJitter()).thenReturn(jitter);
        ExtensionJobParameter extensionJobParameter = new ExtensionJobParameter(mockJobParameter);
        assertEquals(jobName, extensionJobParameter.getName());
        assertEquals(lastUpdateTime, extensionJobParameter.getLastUpdateTime());
        assertEquals(enabledTime, extensionJobParameter.getEnabledTime());
        assertEquals(isEnabled, extensionJobParameter.isEnabled());
        assertEquals(lockDurationSeconds, extensionJobParameter.getLockDurationSeconds());
        assertEquals(jitter, extensionJobParameter.getJitter(), 0.0);
    }

    public void testExtensionJobParameterFromScheduledJobParameterWithNullJitter() {
        String jobName = "test-job";
        Instant lastUpdateTime = Instant.now();
        Instant enabledTime = Instant.now().plusSeconds(3600);
        boolean isEnabled = true;
        Long lockDurationSeconds = 60L;
        ScheduledJobParameter mockJobParameter = mock(ScheduledJobParameter.class);
        when(mockJobParameter.getName()).thenReturn(jobName);
        when(mockJobParameter.getLastUpdateTime()).thenReturn(lastUpdateTime);
        when(mockJobParameter.getEnabledTime()).thenReturn(enabledTime);
        when(mockJobParameter.isEnabled()).thenReturn(isEnabled);
        when(mockJobParameter.getLockDurationSeconds()).thenReturn(lockDurationSeconds);
        when(mockJobParameter.getJitter()).thenReturn(null);
        ExtensionJobParameter extensionJobParameter = new ExtensionJobParameter(mockJobParameter);
        assertEquals(0.0, extensionJobParameter.getJitter(), 0.0);
    }

    public void testGetName() {
        assertEquals(jobName, extensionJobParameter.getName());
    }

    public void testGetLastUpdateTime() {
        assertEquals(lastUpdateTime, extensionJobParameter.getLastUpdateTime());
    }

    public void testGetEnabledTime() {
        assertEquals(enabledTime, extensionJobParameter.getEnabledTime());
    }

    public void testGetSchedule() {
        assertEquals(schedule, extensionJobParameter.getSchedule());
    }

    public void testIsEnabled() {
        assertEquals(isEnabled, extensionJobParameter.isEnabled());
    }

    public void testGetLockDurationSeconds() {
        assertEquals(lockDurationSeconds, extensionJobParameter.getLockDurationSeconds());
    }

    public void testGetJitter() {
        assertEquals(jitter, extensionJobParameter.getJitter());
    }

    public void testToXContent() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder(outputStream);
        xContentBuilder.startObject();
        extensionJobParameter.toXContent(xContentBuilder, null);
        xContentBuilder.endObject();
        xContentBuilder.flush();

        // Parse JSON to compare fields individually (order independent)
        Map<String, Object> actualMap = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            outputStream.toByteArray()
        ).map();

        assertEquals("testJob", actualMap.get("name"));
        assertEquals(true, actualMap.get("enabled"));
        assertEquals(1609459200000L, actualMap.get("last_update_time"));
        assertEquals(1609459200000L, actualMap.get("enabled_time"));
        assertEquals(60, actualMap.get("lock_duration_seconds"));
        assertEquals(0.1, actualMap.get("jitter"));
    }

    public void testExtensionJobParameterConstructor() {
        ScheduledJobParameter jobParameter = new ScheduledJobParameter() {
            @Override
            public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
                return null;
            }

            @Override
            public String getName() {
                return "TestJob";
            }

            @Override
            public Instant getLastUpdateTime() {
                return null;
            }

            @Override
            public Instant getEnabledTime() {
                return null;
            }

            @Override
            public Schedule getSchedule() {
                return null;
            }

            @Override
            public boolean isEnabled() {
                return false;
            }
        };
        ExtensionJobParameter extensionJobParameter = new ExtensionJobParameter(jobParameter);
        assertEquals("TestJob", extensionJobParameter.getName());
        assertEquals(0.0, extensionJobParameter.getJitter(), 0.01); // We can add a delta for double comparison
    }
}
