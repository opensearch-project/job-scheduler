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
import org.opensearch.jobscheduler.transport.request.GetHistoryRequest;
import org.opensearch.jobscheduler.transport.response.GetHistoryResponse;
import org.opensearch.jobscheduler.spi.utils.JobHistoryService;
import org.opensearch.jobscheduler.spi.StatusHistoryModel;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportGetHistoryAction extends HandledTransportAction<GetHistoryRequest, GetHistoryResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetHistoryAction.class);
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetHistoryAction(TransportService transportService, ActionFilters actionFilters, Client client, ThreadPool threadPool) {
        super(GetHistoryAction.NAME, transportService, actionFilters, GetHistoryRequest::new);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, GetHistoryRequest request, ActionListener<GetHistoryResponse> listener) {
        if (request.getHistoryId() != null) {
            getHistoryById(
                request.getHistoryId(),
                ActionListener.wrap(history -> listener.onResponse(new GetHistoryResponse(history)), listener::onFailure)
            );
        } else {
            getAllHistory(ActionListener.wrap(history -> listener.onResponse(new GetHistoryResponse(history)), listener::onFailure));
        }
    }

    private void getHistoryById(String jobId, ActionListener<Map<String, StatusHistoryModel>> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            String[] parts = jobId.split("-", 2);
            if (parts.length != 2) {
                listener.onFailure(new IllegalArgumentException("History ID must be in format 'index-history-id'"));
                return;
            }
            String jobIndexName = parts[0];
            String actualJobId = parts[1];

            SearchRequest searchRequest = new SearchRequest(JobHistoryService.JOB_HISTORY_INDEX_NAME);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("job_index_name", jobIndexName))
                    .must(QueryBuilders.termQuery("job_id", actualJobId))
            );
            searchRequest.source(searchSourceBuilder);

            client.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Map<String, StatusHistoryModel> result = new HashMap<>();
                    searchResponse.getHits().forEach(hit -> {
                        try {
                            XContentParser parser = XContentType.JSON.xContent()
                                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                            parser.nextToken();
                            StatusHistoryModel historyModel = StatusHistoryModel.parse(parser, hit.getSeqNo(), hit.getPrimaryTerm());
                            result.put(hit.getId(), historyModel);
                        } catch (IOException e) {
                            log.error("Error parsing history from search hit", e);
                        }
                    });
                    listener.onResponse(result);
                }

                @Override
                public void onFailure(Exception e) {
                    log.debug("Error in finding history by ID {}", jobId, e);
                    listener.onResponse(new HashMap<>());
                }
            });
        }
    }

    private void getAllHistory(ActionListener<Map<String, StatusHistoryModel>> listener) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
            SearchRequest searchRequest = new SearchRequest(JobHistoryService.JOB_HISTORY_INDEX_NAME);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(1000);
            searchRequest.scroll(TimeValue.timeValueMinutes(1));
            searchRequest.source(searchSourceBuilder);

            client.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Map<String, StatusHistoryModel> allHistory = new HashMap<>();
                    String scrollId = searchResponse.getScrollId();
                    processScrollResults(scrollId, searchResponse, allHistory, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    log.debug("Error in obtaining all history", e);
                    listener.onResponse(new HashMap<>());
                }
            });
        }
    }

    private void processScrollResults(
        String scrollId,
        SearchResponse response,
        Map<String, StatusHistoryModel> allHistory,
        ActionListener<Map<String, StatusHistoryModel>> listener
    ) {
        response.getHits().forEach(hit -> {
            try {
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                parser.nextToken();
                StatusHistoryModel historyModel = StatusHistoryModel.parse(parser, hit.getSeqNo(), hit.getPrimaryTerm());
                allHistory.put(hit.getId(), historyModel);
            } catch (IOException e) {
                log.error("Error parsing history from search hit", e);
            }
        });

        if (response.getHits().getHits().length > 0) {
            client.searchScroll(
                new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(1)),
                new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        processScrollResults(searchResponse.getScrollId(), searchResponse, allHistory, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                        clearScrollRequest.addScrollId(scrollId);
                        client.clearScroll(
                            clearScrollRequest,
                            ActionListener.wrap(r -> {}, ex -> log.warn("Failed to clear scroll context", ex))
                        );
                        log.debug("Error while scrolling for history", e);
                        listener.onResponse(allHistory);
                    }
                }
            );
        } else {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, ActionListener.wrap(r -> {}, e -> log.warn("Failed to clear scroll context", e)));
            listener.onResponse(allHistory);
        }
    }
}
