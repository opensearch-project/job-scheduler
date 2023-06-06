/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JobOperatorParser {

    public static final String JOB = "job";

    public static final String OPERATOR = "operator";

    public static Map<String, XContentBuilder> separateOperatorFromJob(Engine.Index operation, NamedXContentRegistry xContentRegistry) {
        Map<String, XContentBuilder> builderMap = new HashMap<>();
        ParsedDocument parsedDoc = operation.parsedDoc();

        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                BytesReference.toBytes(parsedDoc.source())
            );
            XContentParser.Token token;
            XContentBuilder operatorBuilder = XContentFactory.contentBuilder(parser.contentType());
            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
            builder.startObject();
            operatorBuilder.startObject();
            // the start of the parser
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            String currentFieldName = null;
            while ((token = parser.currentToken()) != null) {
                String tokenName = parser.currentName();
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = tokenName;
                    if (OPERATOR.equals(currentFieldName)) {
                        operatorBuilder.field(currentFieldName);
                        parser.nextToken();
                        operatorBuilder.copyCurrentStructure(parser);
                    } else {
                        builder.field(currentFieldName);
                        parser.nextToken();
                        builder.copyCurrentStructure(parser);
                    }

                } else {
                    parser.nextToken();
                }
            }
            builder.endObject();
            operatorBuilder.endObject();
            builderMap.put(JOB, builder);
            builderMap.put(OPERATOR, operatorBuilder);
            return builderMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
