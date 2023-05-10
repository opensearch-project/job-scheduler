/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.request;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

public class DeleteJobDetailsRequest extends ActionRequest {

    public static String documentId;

    public static final String DOCUMENT_ID = "document_id";

    public DeleteJobDetailsRequest(String documentId) {
        super();
        this.documentId = documentId;
    }

    public static DeleteJobDetailsRequest parse(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DOCUMENT_ID:
                    documentId = parser.textOrNull();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }

        }
        return new DeleteJobDetailsRequest(documentId);

    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

}
