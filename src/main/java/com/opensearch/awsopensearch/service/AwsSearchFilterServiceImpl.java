package com.opensearch.awsopensearch.service;

import com.opensearch.awsopensearch.response_bean.ResponseBean;
import com.opensearch.awsopensearch.common_utils.CommonUtility;
import com.opensearch.awsopensearch.condition.AWSCondition;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.*;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Slf4j
@Service
@Conditional(AWSCondition.class)
public class AwsSearchFilterServiceImpl implements SearchFilterInterface {
    private static final Logger LOGGER = Logger.getLogger(AwsSearchFilterServiceImpl.class.getName());
    private final RestHighLevelClient restHighLevelClient;
    private final CommonUtility commonUtility;

    public AwsSearchFilterServiceImpl(RestHighLevelClient restHighLevelClient,
                                      CommonUtility commonUtility) {
        this.restHighLevelClient = restHighLevelClient;
        this.commonUtility = commonUtility;
    }


    @Override
    public ResponseBean ingestData(String index, Optional<Map<String, Object>> data, Optional<Integer> id, Optional<Integer> limit) throws IOException {
        if (id.isPresent()) {
            IndexRequest request = new IndexRequest(index);
            request.id(String.valueOf(id.get()));
            request.source(data.isPresent() ? data.get() : "");
//            return restHighLevelClient.index(request, RequestOptions.DEFAULT);
            return null;
        } else {
            List<Map<String, Object>> jsonDataToIngest = commonUtility.populateDataToIngest(limit);
            return bulkIngest(jsonDataToIngest, index);
        }
    }

    @Override
    public ResponseBean fetchData(String index, Optional<String> id, Optional<Boolean> isScroll) throws IOException {
        SearchHit[] hits;
        if (id.isPresent()) {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("_id", id.get())));
            hits = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits();
        } else {
            long searchWindowTime = 50;
            int scrollSize = 50;

            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());// Match all documents in the index
            searchSourceBuilder.timeout(TimeValue.timeValueSeconds(searchWindowTime)); // Set a timeout for the search
            searchRequest.source(searchSourceBuilder);

            if(isScroll.isPresent()) {
                searchSourceBuilder.size(scrollSize);// Set the number of documents to return per batch
                SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                String scrollId = searchResponse.getScrollId();
                SearchHit[] searchHits = searchResponse.getHits().getHits();

                while (searchHits != null && searchHits.length > 0) {
                    // Process the search hits here...
                    // Create a new SearchScrollRequest to get the next batch of results
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(TimeValue.timeValueSeconds(30)); // Set the duration of the scroll

                    searchResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();
                }

                // Once all results are processed, clear the scroll context
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);    //to clear scroll
                hits = searchHits;
            } else {
                hits = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits();
            }
        }
        return commonUtility.mapToAWSResponseBean(hits);
    }

    @Override
    public void deleteData(String index) {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOGGER.log(Level.INFO, e.getMessage());
        }
    }



    private ResponseBean bulkIngest(List<Map<String, Object>> jsonDataToIngest, String index) throws IOException {
        BulkRequest request = new BulkRequest();
        for (Map<String, Object> doc : jsonDataToIngest) {
            IndexRequest indexRequest = new IndexRequest(index)
                    .id(String.valueOf(UUID.randomUUID()))
                    .source(doc);
            request.add(indexRequest);
        }
        restHighLevelClient.bulk(request, RequestOptions.DEFAULT);  // data ingestion
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("_id", SortOrder.ASC);
        searchSourceBuilder.size(50);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return commonUtility.mapToAWSResponseBean(search.getHits().getHits()); // data retrieval
    }

}
