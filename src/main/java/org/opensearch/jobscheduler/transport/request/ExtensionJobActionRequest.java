/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionRequest;

/**
 * Request to extensions to invoke a job action, converts request params to a byte array
 *
 */
public class ExtensionJobActionRequest<T extends Writeable> extends ExtensionActionRequest {

    public static final byte UNIT_SEPARATOR = (byte) '\u001F';

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
     * Converts an object of type T that extends {@link Writeable} into a byte array and prepends the fully qualified request class name bytes
     *
     * @param <T> a class that extends writeable
     * @param actionParams the action parameters to be serialized
     * @throws IOException if serialization fails
     * @return the byte array of the parameters
     */
    private static <T extends Writeable> byte[] convertParamsToBytes(T actionParams) throws IOException {

        // Write inner request to output stream and convert to byte array
        BytesStreamOutput out = new BytesStreamOutput();
        actionParams.writeTo(out);
        out.flush();
        byte[] requestBytes = BytesReference.toBytes(out.bytes());

        // Convert fully qualifed class name to byte array
        byte[] requestClassBytes = actionParams.getClass().getName().getBytes(StandardCharsets.UTF_8);

        // Generate ExtensionActionRequest responseByte array
        byte[] proxyRequestBytes = ByteBuffer.allocate(requestClassBytes.length + 1 + requestBytes.length)
            .put(requestClassBytes)
            .put(ExtensionJobActionRequest.UNIT_SEPARATOR)
            .put(requestBytes)
            .array();

        return proxyRequestBytes;
    }

}
