/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import org.junit.Before;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class RunJobRequestTests extends OpenSearchTestCase {

    private static final String JOB_TYPE = "my-job-type";
    private static final String JOB_ID = "my-job-id";

    private RunJobRequest request;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        request = new RunJobRequest(JOB_TYPE, JOB_ID);
    }
    public void testValidate_withNullJobType_returnError() {
        RunJobRequest invalidRequest = new RunJobRequest(null, JOB_ID);
        ActionRequestValidationException ex = invalidRequest.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("job_type is required"));
    }

    public void testValidate_withEmptyJobType_returnError() {
        RunJobRequest invalidRequest = new RunJobRequest("", JOB_ID);
        ActionRequestValidationException ex = invalidRequest.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("job_type is required"));
    }

    public void testValidate_withNullJobId_returnError() {
        RunJobRequest invalidRequest = new RunJobRequest(JOB_TYPE, null);
        ActionRequestValidationException ex = invalidRequest.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("job_id is required"));
    }

    public void testValidate_withEmptyJobId_returnError() {
        RunJobRequest invalidRequest = new RunJobRequest(JOB_TYPE, "");
        ActionRequestValidationException ex = invalidRequest.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("job_id is required"));
    }

    public void testValidate_withBothParamsNull_returnBothErrors() {
        RunJobRequest invalidRequest = new RunJobRequest(null, null);
        ActionRequestValidationException ex = invalidRequest.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("job_type is required"));
        assertTrue(ex.getMessage().contains("job_id is required"));
    }

}
