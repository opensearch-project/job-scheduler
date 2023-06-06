/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.utils;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.Term;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobOperatorParserTests extends OpenSearchTestCase {

    private Term newUid(ParsedDocument doc) {
        return new Term("_id", Uid.encodeId(doc.id()));
    }

    public void testSeparateOperatorFromJob() throws IOException {
        long randomPrimaryTerm = randomBoolean() ? 0 : randomNonNegativeLong();
        Map<String, Object> job = new HashMap<>();
        job.put("name", "test-job-1");
        Map<String, Object> interval = new HashMap<>();
        interval.put("start_time", Instant.now().toEpochMilli());
        interval.put("period", 10);
        interval.put("unit", TimeUnit.MINUTES.toString());
        job.put("schedule", Map.of("interval", interval));
        job.put("enabled", true);
        Map<String, Object> user = new HashMap<>();
        user.put("name", "username1");
        user.put("roles", List.of("all_access"));
        user.put("backend_roles", List.of("backend_role"));
        job.put("operator", Map.of("user", user));
        BytesReference source = BytesReference.bytes(JsonXContent.contentBuilder().map(job));
        ParsedDocument jobDocument = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            "id",
            "routingValue",
            null,
            source,
            XContentType.JSON,
            null
        );
        Engine.Index operation = new Engine.Index(newUid(jobDocument), randomPrimaryTerm, jobDocument);
        Map<String, XContentBuilder> jobAndOperatorBuilders = JobOperatorParser.separateOperatorFromJob(
            operation,
            NamedXContentRegistry.EMPTY
        );

        XContentBuilder jobBuilder = jobAndOperatorBuilders.get(JobOperatorParser.JOB);
        assertNotNull(jobBuilder);
        XContentBuilder operatorBuilder = jobAndOperatorBuilders.get(JobOperatorParser.OPERATOR);
        assertNotNull(operatorBuilder);

        String jobContent = Strings.toString(jobBuilder);
        assertFalse(jobContent.contains("operator"));
        assertFalse(jobContent.contains("user"));

        String operatorContent = Strings.toString(operatorBuilder);
        assertFalse(operatorContent.contains("schedule"));
    }
}
