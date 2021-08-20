/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.jobscheduler.spi.utils;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import java.io.IOException;


public class SchemaUtilsTests extends OpenSearchTestCase {

    public void testGetSchemaVersion() {
        String message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": 2}}";
        try {
            long schemaVersion = SchemaUtils.schemaVersion(message);
            // This must be updated every time the opensearch_job_scheduler_lock.json is changed.
            long currentSchemaVersion = 2L;
            assertEquals(currentSchemaVersion, schemaVersion);
        }
        catch (IOException exception) {
            fail(exception.getMessage());
        }
    }

    public void testGetSchemaWithoutSchemaVersion() {
        String message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"test\": 1}}";
        try {
            long schemaVersion = SchemaUtils.schemaVersion(message);
            assertEquals(1L, schemaVersion);
        }
        catch (IOException exception) {
            fail(exception.getMessage());
        }
    }

    public void testGetSchemaWithWrongSchemaVersion() {
        String message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": \"wrong\"}}";
        try {
            // just attempt getting the schema version
            SchemaUtils.schemaVersion(message);
        }
        catch (Exception exception) {
            assertTrue(exception instanceof IllegalArgumentException);
        }
    }

    public void testShouldUpdateIndexWithoutOriginalVersion() {
        String indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
                "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
                "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
                "\"settings_version\":123,\"mappings\":{\"_doc\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}";
        try {
            // just attempt getting the schema version
            XContentParser parser = createParser(XContentType.JSON.xContent(), indexContent);
            IndexMetadata index = IndexMetadata.fromXContent(parser);
            assertTrue(SchemaUtils.shouldUpdateIndex(index, 10));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    public void testShouldUpdateIndexWithLaggedVersion() {
        String indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
                "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
                "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
                "\"settings_version\":123,\"mappings\":{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":" +
                "{\"name\":{\"type\":\"keyword\"}}}}}}";
        try {
            // just attempt getting the schema version
            XContentParser parser = createParser(XContentType.JSON.xContent(), indexContent);
            IndexMetadata index = IndexMetadata.fromXContent(parser);
            assertTrue(SchemaUtils.shouldUpdateIndex(index, 10));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    public void testShouldUpdateIndexWithSameVersion() {
        String indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
                "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
                "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
                "\"settings_version\":123,\"mappings\":{\"_doc\":{\"_meta\":{\"schema_version\":10},\"properties\":" +
                "{\"name\":{\"type\":\"keyword\"}}}}}}";
        try {
            // just attempt getting the schema version
            XContentParser parser = createParser(XContentType.JSON.xContent(), indexContent);
            IndexMetadata index = IndexMetadata.fromXContent(parser);
            assertFalse(SchemaUtils.shouldUpdateIndex(index, 10));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }
}
