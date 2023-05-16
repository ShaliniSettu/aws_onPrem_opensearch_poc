package com.opensearch.awsopensearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensearch.awsopensearch.response_bean.ResponseBean;
import com.opensearch.awsopensearch.common_utils.CommonUtility;
import com.opensearch.awsopensearch.condition.OnPremCondition;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldSort;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.*;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Slf4j
@Service
@Conditional(OnPremCondition.class)
public class OnPremSearchFilterServiceImpl implements SearchFilterInterface {
    private static final Logger LOGGER = Logger.getLogger(OnPremSearchFilterServiceImpl.class.getName());
    private final ObjectMapper objectMapper;
    private final OpenSearchClient searchClient;
    private final CommonUtility commonUtility;

    public OnPremSearchFilterServiceImpl(ObjectMapper objectMapper, OpenSearchClient searchClient, CommonUtility commonUtility) {
        this.objectMapper = objectMapper;
        this.searchClient = searchClient;
        this.commonUtility = commonUtility;
    }

    @Override
    public ResponseBean ingestData(String index, Optional<Map<String, Object>> data, Optional<Integer> id, Optional<Integer> limit) throws Exception {
        List<Map<String, Object>> jsonDataToIngest;
        if (id.isPresent()) {
            jsonDataToIngest = (List<Map<String, Object>>) data.get();
        } else {
            jsonDataToIngest = commonUtility.populateDataToIngest(limit);
        }
        return bulkIngest(jsonDataToIngest, index);
    }

    private ResponseBean bulkIngest(List<Map<String, Object>> bulkData, String index) throws Exception {
        List<BulkOperation> operations = new ArrayList<>();
        for (Map<String, Object> doc : bulkData) {
            try {
                String json = objectMapper.writeValueAsString(doc);
                JsonNode node = objectMapper.readTree(json);
                BulkOperation operation =
                        new BulkOperation.Builder()
                                .index(
                                        i -> i.index(index).id(String.valueOf(UUID.randomUUID())).document(node))
                                .build();
                operations.add(operation);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        BulkRequest bulkRequest =
                new BulkRequest.Builder().index(index).operations(operations).build();
        BulkResponse bulkResponse = searchClient.bulk(bulkRequest);
        if (bulkResponse.errors()
                && bulkResponse.items() != null
                && !bulkResponse.items().isEmpty()
                && bulkResponse.items().get(0).error() != null) {
            LOGGER.log(Level.INFO, "bulkResponse error Message :: " + bulkResponse.items().get(0).error().reason());
            throw new Exception("bulkResponse error Message :: " + bulkResponse.items().get(0).error().reason());
        } else {
            List<SortOptions> sortOptionsList = buildDefaultSort();
            Query finalQuery = buildDefaultQuery();
            List<Hit<Map>> hits = searchClient.search(s -> s.index(index).sort(sortOptionsList).query(finalQuery).size(50), Map.class).hits().hits();
            return commonUtility.mapToOn_PremResponseBean(hits);
        }
    }

    private Query buildDefaultQuery() {
        return new BoolQuery.Builder().must(new ArrayList<>()).build()._toQuery();
    }

    private List<SortOptions> buildDefaultSort() {
        List<SortOptions> sortOptionList = new ArrayList<>();
        FieldSort fieldSort =
                new FieldSort.Builder()
                        .field("_id")
                        .order(SortOrder.Asc)
                        .build();
        SortOptions sortOption = new SortOptions.Builder().field(fieldSort).build();
        sortOptionList.add(sortOption);
        return sortOptionList;
    }

    @Override
    public ResponseBean fetchData(String index, Optional<String> id, Optional<Boolean> isScroll) throws Exception {
        List<Hit<Map>> hits;
        if (id.isPresent()) {
            List<FieldValue> fieldValueList = Collections.singletonList(FieldValue.of(id.get().trim()));
            Query query = new TermsQuery.Builder()
                    .field("_id")
                    .terms(new TermsQueryField.Builder().value(fieldValueList).build())
                    .build()
                    ._toQuery();
            hits = searchClient.search(s -> s.index(index).query(query), Map.class).hits().hits();
        } else {
            if(isScroll.isPresent()) {
                String searchWindowTime = "50m";
                int scrollSize = 50;
                SearchResponse<Map> searchResponse = searchClient.search(
                        s -> s.index(index).scroll(sc -> sc.time(searchWindowTime)).size(scrollSize),
                        Map.class);

                if (searchResponse != null
                        && searchResponse.hits() != null
                        && !searchResponse.hits().hits().isEmpty()
                        && searchResponse.scrollId() != null) {
                    hits = searchResponse.hits().hits();
                } else {
                    throw new Exception("Search data not found in open search");
                }
            } else {
                hits = searchClient.search(s -> s.index(index), Map.class).hits().hits();
            }
        }
        return commonUtility.mapToOn_PremResponseBean(hits);
    }

    @Override
    public void deleteData(String index) {
        try {
            searchClient.indices().delete(i -> i.index(index));
        } catch (IOException e) {
            LOGGER.log(Level.INFO, e.getMessage());
        }
    }
}
