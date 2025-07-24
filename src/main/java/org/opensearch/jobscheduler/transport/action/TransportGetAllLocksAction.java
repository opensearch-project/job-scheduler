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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.request.GetAllLocksRequest;
import org.opensearch.jobscheduler.transport.response.GetAllLocksResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportGetAllLocksAction extends HandledTransportAction<GetAllLocksRequest, GetAllLocksResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetAllLocksAction.class);
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetAllLocksAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool
    ) {
        super(GetAllLocksAction.NAME, transportService, actionFilters, GetAllLocksRequest::new);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, GetAllLocksRequest request, ActionListener<GetAllLocksResponse> listener) {
        getAllLocks(ActionListener.wrap(locks -> listener.onResponse(new GetAllLocksResponse(locks)), listener::onFailure));
    }

    private void getAllLocks(ActionListener<Map<String, LockModel>> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            SearchRequest searchRequest = new SearchRequest(LockService.LOCK_INDEX_NAME);
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
