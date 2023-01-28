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
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.jobscheduler.utils.JobDetailsService;

/**
 * Request to extensions to invoke a job action, converts request params to a byte array
 *
 */
public class ExtensionJobActionRequest<T extends Writeable> extends ExtensionActionRequest {

    /**
     * Instantiates a new ExtensionJobActionRequest
     *
     * @param extensionActionName the extension action to invoke
     * @param actionParams the request object holding the action parameters
     * @throws IOException if serialization fails
     */
    public ExtensionJobActionRequest(String extensionActionName, T actionParams) throws IOException {
        super(extensionActionName, JobDetailsService.convertParamsToBytes(actionParams));
    }

}
