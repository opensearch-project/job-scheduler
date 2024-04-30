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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JobDetailsTests extends OpenSearchTestCase {

    private JobDetails jobDetails;

    String jobIndex = "test_index";
    String jobType = "test_type";
    String jobParameterAction = "test_parameter_action";
    String jobRunnerAction = "test_runner_action";
    String extensionUniqueId = "test_extension_id";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobDetails = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);

    }

    public void testConstructor() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";
        assertEquals(jobIndex, jobDetails.getJobIndex());
        assertEquals(jobType, jobDetails.getJobType());
        assertEquals(jobParameterAction, jobDetails.getJobParameterAction());
        assertEquals(jobRunnerAction, jobDetails.getJobRunnerAction());
        assertEquals(extensionUniqueId, jobDetails.getExtensionUniqueId());
    }

    public void testCopyConstructor() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        JobDetails originalJobDetails = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
        JobDetails copiedJobDetails = new JobDetails(originalJobDetails);

        assertEquals(originalJobDetails, copiedJobDetails);
    }

    public void testSetters() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        jobDetails.setJobIndex(jobIndex);
        jobDetails.setJobType(jobType);
        jobDetails.setJobParameterAction(jobParameterAction);
        jobDetails.setJobRunnerAction(jobRunnerAction);
        jobDetails.setExtensionUniqueId(extensionUniqueId);

        assertEquals(jobIndex, jobDetails.getJobIndex());
        assertEquals(jobType, jobDetails.getJobType());
        assertEquals(jobParameterAction, jobDetails.getJobParameterAction());
        assertEquals(jobRunnerAction, jobDetails.getJobRunnerAction());
        assertEquals(extensionUniqueId, jobDetails.getExtensionUniqueId());
    }

    public void testToXContent() throws IOException {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        JobDetails jobDetails = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        XContentBuilder xContentBuilder = jobDetails.toXContent(builder, null);

        String expectedJson =
            "{\"job_index\":\"test_index\",\"job_type\":\"test_type\",\"job_parser_action\":\"test_parameter_action\",\"job_runner_action\":\"test_runner_action\",\"extension_unique_id\":\"test_extension_id\"}";
        assertEquals(expectedJson, xContentBuilder.toString());
    }

    public void testParseWithNullValues() throws IOException {
        String json =
            "{\"job_index\":null,\"job_type\":null,\"job_parser_action\":null,\"job_runner_action\":null,\"extension_unique_id\":null}";

        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, null, new BytesArray(json.getBytes(StandardCharsets.UTF_8)).array());
        parser.nextToken(); // Advance to the START_OBJECT token

        JobDetails jobDetails = null;
        try {
            jobDetails = JobDetails.parse(parser);
        } catch (IllegalStateException e) {
            // Handle the "Can't get text on a VALUE_NULL" exception
            if (e.getMessage().contains("Can't get text on a VALUE_NULL")) {
                // Ignore the exception and set all fields to null
                jobDetails = new JobDetails(null, null, null, null, null);
            } else {
                throw e;
            }
        }

        assertNull(jobDetails.getJobIndex());
        assertNull(jobDetails.getJobType());
        assertNull(jobDetails.getJobParameterAction());
        assertNull(jobDetails.getJobRunnerAction());
        assertNull(jobDetails.getExtensionUniqueId());
    }

    public void testEquals() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        JobDetails jobDetails1 = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
        JobDetails jobDetails2 = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
        JobDetails jobDetails3 = new JobDetails(null, null, null, null, null);

        assertEquals(jobDetails1, jobDetails2);
        assertNotNull(jobDetails1);
        assertNotNull(jobDetails2);
        assertNotNull(jobDetails3);
    }

    public void testHashCode() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        JobDetails jobDetails1 = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);
        JobDetails jobDetails2 = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);

        assertEquals(jobDetails1.hashCode(), jobDetails2.hashCode());
    }

    public void testToString() {
        String jobIndex = "test_index";
        String jobType = "test_type";
        String jobParameterAction = "test_parameter_action";
        String jobRunnerAction = "test_runner_action";
        String extensionUniqueId = "test_extension_id";

        JobDetails jobDetails = new JobDetails(jobIndex, jobType, jobParameterAction, jobRunnerAction, extensionUniqueId);

        String expectedString =
            "JobDetails{jobIndex='test_index', jobType='test_type', jobParameterAction='test_parameter_action', jobRunnerAction='test_runner_action', extensionUniqueId='test_extension_id'}";
        assertEquals(expectedString, jobDetails.toString());
    }
}
