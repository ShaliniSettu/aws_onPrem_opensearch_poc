//package com.opensearch.awsopensearch.service;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
//import org.opensearch.action.bulk.BulkRequest;
//import org.opensearch.action.bulk.BulkResponse;
//import org.opensearch.action.get.GetRequest;
//import org.opensearch.action.index.IndexRequest;
//import org.opensearch.action.search.*;
//import org.opensearch.client.RequestOptions;
//import org.opensearch.client.RestHighLevelClient;
//import org.opensearch.common.unit.TimeValue;
//import org.opensearch.index.query.QueryBuilders;
//import org.opensearch.search.SearchHit;
//import org.opensearch.search.builder.SearchSourceBuilder;
//import org.opensearch.search.sort.SortOrder;
//import org.springframework.stereotype.Service;
//
//import java.io.IOException;
//import java.util.*;
//
//@Service
//public class AwsOSService {
//    private final ObjectMapper mapper = new ObjectMapper();
//    private final RestHighLevelClient searchClient;
//
//    public AwsOSService(RestHighLevelClient searchClient) {
//        this.searchClient = searchClient;
//    }
//
//    public Object ingestData(String index, Optional<Map<String,Object>> data,
//                             Optional<Integer> id,
//                             Optional<Integer> limit) throws IOException {
//        if (id.isPresent()) {
//            IndexRequest request = new IndexRequest(index);
//            request.id(String.valueOf(id.get()));
//            request.source(data.isPresent() ? data.get() : "");
//            return searchClient.index(request, RequestOptions.DEFAULT);
//        } else {
//            List<Map<String, Object>> jsonDataToIngest = populateDataToIngest(limit);
//            return bulkIngest(jsonDataToIngest, index);
//        }
//    }
//
//    public Object fetchData(String index, Optional<String> id, Optional<Boolean> isScroll) throws IOException {
//        if (id.isPresent()) {
//            GetRequest getRequest = new GetRequest(index);
//            getRequest.id(id.get());
//            return searchClient.get(getRequest, RequestOptions.DEFAULT).getSource();
//        } else {
//            SearchRequest searchRequest = new SearchRequest(index);
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.query(QueryBuilders.matchAllQuery());// Match all documents in the index
//            searchSourceBuilder.timeout(TimeValue.timeValueSeconds(50)); // Set a timeout for the search
//            searchRequest.source(searchSourceBuilder);
//
//            if(isScroll.isPresent()) {
//                searchSourceBuilder.size(50);// Set the number of documents to return per batch
//                SearchResponse searchResponse = searchClient.search(searchRequest, RequestOptions.DEFAULT);
//                String scrollId = searchResponse.getScrollId();
//                SearchHit[] searchHits = searchResponse.getHits().getHits();
//
//                while (searchHits != null && searchHits.length > 0) {
//                    // Process the search hits here...
//                    // Create a new SearchScrollRequest to get the next batch of results
//                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
//                    scrollRequest.scroll(TimeValue.timeValueSeconds(30)); // Set the duration of the scroll
//
//                    searchResponse = searchClient.scroll(scrollRequest, RequestOptions.DEFAULT);
//                    scrollId = searchResponse.getScrollId();
//                    searchHits = searchResponse.getHits().getHits();
//                }
//
//                // Once all results are processed, clear the scroll context
//                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//                clearScrollRequest.addScrollId(scrollId);
//                ClearScrollResponse clearScrollResponse = searchClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
//                boolean succeeded = clearScrollResponse.isSucceeded();
//                System.out.println(succeeded);
//
//                return searchHits;
//            } else {
//                SearchRequest searchRequest = new SearchRequest("your_index");
//                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//                searchSourceBuilder.sort("your_field", SortOrder.ASC);
//                searchSourceBuilder.from(page * PAGE_SIZE);
//                searchSourceBuilder.size(PAGE_SIZE);
//                searchRequest.source(searchSourceBuilder);
//
//                return searchClient.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits();
//            }
//        }
//    }
//
//    public void deleteData(String index) throws IOException {
//        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
//        searchClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
//    }
//
//    private BulkResponse bulkIngest(List<Map<String, Object>> jsonDataToIngest, String index) throws IOException {
//        BulkRequest request = new BulkRequest();
//        for (Map<String, Object> doc : jsonDataToIngest) {
//            IndexRequest indexRequest = new IndexRequest(index)
//                    .id(String.valueOf(UUID.randomUUID()))
//                    .source(doc);
//            request.add(indexRequest);
//        }
//        return searchClient.bulk(request, RequestOptions.DEFAULT);
//    }
//
//    private List<Map<String, Object>> populateDataToIngest(Optional<Integer> limit) {
//        List<Map<String, Object>> source = new ArrayList<>();
//        for (int i = 1; i <= limit.get(); i++) {
//            Map<String, Object> jsonMap = new HashMap<>();
//            jsonMap.put("Who", "Actor_"+UUID.randomUUID());
//            jsonMap.put("manufacturer", "Benz");
//            jsonMap.put("model", "A Class");
//            jsonMap.put("description", "Latest Model");
//            source.add(jsonMap);
//        }
//        return source;
//    }
//}
