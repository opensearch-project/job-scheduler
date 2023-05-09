/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.http.entity.ContentType;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

public class TestHelpers {

    public static final String GET_JOB_DETAILS_BASE_URI = "/_plugins/_job_scheduler/_job_details";
    public static final String GET_LOCK_BASE_URI = "/_plugins/_job_scheduler/_lock";
    public static final String RELEASE_LOCK_BASE_URI = "/_plugins/_job_scheduler/_release_lock";

    public static String xContentBuilderToString(XContentBuilder builder) {
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(object.toXContent(builder, ToXContent.EMPTY_PARAMS));
    }

    public static HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), ContentType.APPLICATION_JSON);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    public static HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    public static String generateAcquireLockRequestBody(String jobIndexName, String jobId) {
        return "{\"job_id\":\"" + jobId + "\",\"job_index_name\":\"" + jobIndexName + "\",\"lock_duration_seconds\":\"30.0\"}";
    }

    public static String generateExpectedLockId(String jobIndexName, String jobId) {
        return jobIndexName + "-" + jobId;
    }

}
