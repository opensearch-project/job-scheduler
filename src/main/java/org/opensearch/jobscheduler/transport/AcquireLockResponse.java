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

import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.LockModel;

import static java.util.Objects.requireNonNull;

/**
 * Response class used to facilitate serialization/deserialization of the GetLock response
 */
public class AcquireLockResponse implements ToXContentObject {
    private final LockModel lock;
    private final String lockId;

    public AcquireLockResponse(final LockModel lock, final String lockId) {
        this.lock = lock;
        this.lockId = lockId;
    }

    public LockModel getLock() {
        return this.lock;
    }

    public String getLockId() {
        return this.lockId;
    }

    public static AcquireLockResponse parse(final XContentParser parser) throws IOException {
        LockModel lock = null;
        String lockId = null;
        Long seqNo = null;
        Long primaryTerm = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case LockModel.LOCK_ID:
                    lockId = parser.text();
                    break;
                case LockModel.LOCK_MODEL:
                    lock = LockModel.parse(parser);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }
        return new AcquireLockResponse(requireNonNull(lock, "LockModel cannot be null"), requireNonNull(lockId, "LockId cannot be null"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LockModel.LOCK_ID, lockId);
        builder.field(LockModel.LOCK_MODEL, lock);
        builder.endObject();
        return builder;
    }

}
