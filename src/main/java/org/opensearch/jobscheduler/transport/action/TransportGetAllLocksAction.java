/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.transport.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.transport.PluginClient;
import org.opensearch.jobscheduler.transport.request.GetLocksRequest;
import org.opensearch.jobscheduler.transport.response.GetLocksResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.jobscheduler.utils.LockServiceImpl.LOCK_INDEX_NAME;

public class TransportGetAllLocksAction extends HandledTransportAction<GetLocksRequest, GetLocksResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetAllLocksAction.class);
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetAllLocksAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PluginClient client,
        ThreadPool threadPool
    ) {
        super(GetAllLocksAction.NAME, transportService, actionFilters, GetLocksRequest::new);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, GetLocksRequest request, ActionListener<GetLocksResponse> listener) {
        if (request.getLockId() != null) {
            getLockById(
                request.getLockId(),
                ActionListener.wrap(locks -> listener.onResponse(new GetLocksResponse(locks)), listener::onFailure)
            );
        } else {
            getAllLocks(ActionListener.wrap(locks -> listener.onResponse(new GetLocksResponse(locks)), listener::onFailure));
        }
    }

    private void getLockById(String lockId, ActionListener<Map<String, LockModel>> listener) {
        String[] parts = lockId.split("-", 2);
        if (parts.length != 2) {
            listener.onFailure(new IllegalArgumentException("Lock ID must be in format 'index-jobid'"));
            return;
        }
        String jobIndexName = parts[0];
        String jobId = parts[1];

        SearchRequest searchRequest = new SearchRequest(LOCK_INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("job_index_name", jobIndexName))
                .must(QueryBuilders.termQuery("job_id", jobId))
        );
        searchRequest.source(searchSourceBuilder);

        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Map<String, LockModel> result = new HashMap<>();
                searchResponse.getHits().forEach(hit -> {
                    try {
                        XContentParser parser = XContentType.JSON.xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                        parser.nextToken();
                        LockModel lock = LockModel.parse(parser, hit.getSeqNo(), hit.getPrimaryTerm());
                        result.put(lock.getLockId(), lock);
                    } catch (IOException e) {
                        log.error("Error parsing lock from search hit", e);
                    }
                });
                listener.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                log.debug("Error in finding lock by ID {}", lockId, e);
                listener.onResponse(new HashMap<>());
            }
        });
    }

    private void getAllLocks(ActionListener<Map<String, LockModel>> listener) {
        SearchRequest searchRequest = new SearchRequest(LOCK_INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1000); // Set a reasonable batch size
        searchRequest.scroll(TimeValue.timeValueMinutes(1)); // Set scroll timeout
        searchRequest.source(searchSourceBuilder);

        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Map<String, LockModel> allLocks = new HashMap<>();
                String scrollId = searchResponse.getScrollId();
                processScrollResults(scrollId, searchResponse, allLocks, listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.debug("Error in obtaining all locks", e);
                listener.onResponse(new HashMap<>());
            }
        });
    }

    private void processScrollResults(
        String scrollId,
        SearchResponse response,
        Map<String, LockModel> allLocks,
        ActionListener<Map<String, LockModel>> listener
    ) {
        // Process current batch of results
        response.getHits().forEach(hit -> {
            try {
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                parser.nextToken();
                LockModel lock = LockModel.parse(parser, hit.getSeqNo(), hit.getPrimaryTerm());
                allLocks.put(lock.getLockId(), lock);
            } catch (IOException e) {
                log.error("Error parsing lock from search hit", e);
            }
        });

        // Check if we need to continue scrolling
        if (response.getHits().getHits().length > 0) {
            // Continue scrolling
            client.searchScroll(
                new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(1)),
                new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        processScrollResults(searchResponse.getScrollId(), searchResponse, allLocks, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Clear the scroll context to free resources
                        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                        clearScrollRequest.addScrollId(scrollId);
                        client.clearScroll(
                            clearScrollRequest,
                            ActionListener.wrap(r -> {}, ex -> log.warn("Failed to clear scroll context", ex))
                        );
                        log.debug("Error while scrolling for locks", e);
                        // Return what we have so far
                        listener.onResponse(allLocks);
                    }
                }
            );
        } else {
            // No more results, clear the scroll context
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, ActionListener.wrap(r -> {}, e -> log.warn("Failed to clear scroll context", e)));
            // Return all collected results
            listener.onResponse(allLocks);
        }
    }
}
