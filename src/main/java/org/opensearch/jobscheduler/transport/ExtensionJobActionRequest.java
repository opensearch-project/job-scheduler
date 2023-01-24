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

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionRequest;

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
        super(extensionActionName, convertParamsToBytes(actionParams));
    }

    /**
     * Takes in an object of type T that extends {@link Writeable} and converts the request to a byte array
     *
     * @param <T> a class that extends writeable
     * @param actionParams the action parameters to be serialized
     * @throws IOException if serialization fails
     * @return the byte array of the parameters
     */
    public static <T extends Writeable> byte[] convertParamsToBytes(T actionParams) throws IOException {
        // Write all to output stream
        BytesStreamOutput out = new BytesStreamOutput();
        actionParams.writeTo(out);
        out.flush();

        // convert bytes stream to byte array
        return BytesReference.toBytes(out.bytes());
    }
}
