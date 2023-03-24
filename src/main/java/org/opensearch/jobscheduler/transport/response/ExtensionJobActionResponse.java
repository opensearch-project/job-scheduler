/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.response;

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
        super(convertParamsToBytes(actionResponse));
    }

    /**
     * Takes in an object of type T that extends {@link Writeable} and converts the writeable fields to a byte array
     *
     * @param <T> a class that extends writeable
     * @param actionParams the action parameters to be serialized
     * @throws IOException if serialization fails
     * @return the byte array of the parameters
     */
    private static <T extends Writeable> byte[] convertParamsToBytes(T actionParams) throws IOException {
        // Write all to output stream
        BytesStreamOutput out = new BytesStreamOutput();
        actionParams.writeTo(out);
        out.flush();

        // convert bytes stream to byte array
        return BytesReference.toBytes(out.bytes());
    }

}
