/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.jobscheduler;

 import org.opensearch.common.bytes.BytesReference;
 import org.opensearch.common.xcontent.XContentBuilder;
 import org.opensearch.common.xcontent.XContentType;
 
 import java.io.IOException;
 
 public class TestHelpers {
 
     public static XContentBuilder builder() throws IOException {
         return XContentBuilder.builder(XContentType.JSON.xContent());
     }
 
     public static String xContentBuilderToString(XContentBuilder builder) {
         return BytesReference.bytes(builder).utf8ToString();
     }
 }