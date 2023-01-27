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
import org.opensearch.extensions.action.ExtensionActionResponse;

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
        super(convertResponseToBytes(actionResponse));
    }

    /**
     * Takes in an object of type T that extends {@link Writeable} and converts the response to a byte array
     *
     * @param <T> a class that extends writeable
     * @param actionResponse the action parameters to be serialized
     * @throws IOException if serialization fails
     * @return the byte array of the parameters
     */
    public static <T extends Writeable> byte[] convertResponseToBytes(T actionResponse) throws IOException {
        // Write all to output stream
        BytesStreamOutput out = new BytesStreamOutput();
        actionResponse.writeTo(out);
        out.flush();

        // convert bytes stream to byte array
        return BytesReference.toBytes(out.bytes());
    }
}
