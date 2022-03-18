/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParser.Token;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SchemaUtils {
    private static final String _META = "_meta";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String _DOC = "_doc";
    private static final long DEFAULT_SCHEMA_VERSION = 1L;
    private static final Logger logger = LogManager.getLogger(SchemaUtils.class);

    public static long getSchemaVersion(String mapping) throws IOException {
        XContentParser xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, mapping);
        while (!xcp.isClosed()) {
            Token token = xcp.currentToken();
            if (token != null && token != Token.END_OBJECT && token != Token.START_OBJECT) {
                if (!xcp.currentName().equals(_META)) {
                    xcp.nextToken();
                    xcp.skipChildren();
                } else {
                    while (xcp.nextToken() != Token.END_OBJECT) {
                        if (xcp.currentName().equals(SCHEMA_VERSION)) {
                            return xcp.longValue();
                        }
                        else {
                            xcp.nextToken();
                        }
                    }
                }
            }
            xcp.nextToken();
        }
        return DEFAULT_SCHEMA_VERSION;
    }

    public static boolean shouldUpdateIndex(IndexMetadata index, long newVersion) {
        long oldVersion = DEFAULT_SCHEMA_VERSION;
        Map<String, Object> indexMapping = index.mapping().sourceAsMap();
        if (indexMapping != null && indexMapping.containsKey(_META)) {
            Object meta = indexMapping.get(_META);
            if (meta instanceof HashMap) {
                HashMap<?, ?> metaData = (HashMap<?, ?>) meta;
                if (metaData.containsKey(SCHEMA_VERSION)) {
                    Object oldVersionObject = metaData.get(SCHEMA_VERSION);
                    if (oldVersionObject instanceof Long) {
                        oldVersion = (Long) oldVersionObject;
                    }
                    if (oldVersionObject instanceof Integer) {
                        Integer version = (Integer) oldVersionObject;
                        oldVersion = version.longValue();
                    }
                }
            }
        }
        return newVersion > oldVersion;
    }

    public static void checkAndUpdateLockIndexMapping(String mapping, ClusterService clusterService,
                                                      IndicesAdminClient client, ActionListener<AcknowledgedResponse> listener) {
        ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().indices().containsKey(LockService.LOCK_INDEX_NAME)) {
            try {
                long newSchemaVersion = getSchemaVersion(mapping);
                IndexMetadata indexMetadata = clusterState.metadata().indices().get(LockService.LOCK_INDEX_NAME);
                if (shouldUpdateIndex(indexMetadata, newSchemaVersion)) {
                    PutMappingRequest putMappingRequest = new PutMappingRequest(LockService.LOCK_INDEX_NAME).type(_DOC).source(mapping, XContentType.JSON);
                    client.putMapping(putMappingRequest, listener);
                } else {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            } catch (Exception e) {
                listener.onResponse(new AcknowledgedResponse(false));
            }
        } else {
            logger.error("Job scheduler lock index does not exist.");
            listener.onResponse(new AcknowledgedResponse(false));
        }
    }
}
