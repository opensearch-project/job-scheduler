/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport;

import java.io.IOException;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.utils.JobDetailsService;

/**
 * Response from extension job action, converts response params to a byte array
 *
 */
public class ExtensionJobActionResponse<T extends Writeable> extends ExtensionActionResponse {

    /**
     * Instantiates a new ExtensionJobActionResponse
     *
     * @param actionResponse the response object holding the action response parameters
     * @throws IOException if serialization fails
     */
    public ExtensionJobActionResponse(T actionResponse) throws IOException {
        super(JobDetailsService.convertParamsToBytes(actionResponse));
    }

}
