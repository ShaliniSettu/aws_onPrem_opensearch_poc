package com.opensearch.awsopensearch.common_utils;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensearch.awsopensearch.response_bean.ResponseBean;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.search.SearchHit;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class CommonUtility {
    public static final String RECORD_ID = "RESULT_ROW_ID";
    private final ObjectMapper objectMapper;

    public CommonUtility(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ResponseBean mapToAWSResponseBean(SearchHit[] hits) {
        List<Map<String, Object>> results = new ArrayList<>();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            sourceAsMap.put(RECORD_ID, hit.getId());
            results.add(sourceAsMap);
        }
        String index = Arrays.stream(hits).iterator().next().getIndex();
        return ResponseBean.builder().results(results).index(index).build();
    }

    public ResponseBean mapToOn_PremResponseBean(List<Hit<Map>> hits) {
        List<Map<String, Object>> results = new ArrayList<>();

        for (Hit<Map> hit : hits) {
            Map<String, Object> source = hit.source();
            source.put(RECORD_ID, hit.id());
            results.add(source);
        }
        String index = hits.stream().iterator().next().index();
        ResponseBean build = ResponseBean.builder().results(results).index(index).build();
        return build;
    }

    public List<Map<String, Object>> populateDataToIngest(Optional<Integer> limit) {
        List<Map<String, Object>> source = new ArrayList<>();
        for (int i = 1; i <= limit.get(); i++) {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("Who", "Actor_" + UUID.randomUUID());
            jsonMap.put("manufacturer", "Benz");
            jsonMap.put("model", "A Class");
            jsonMap.put("description", "Latest Model");
            source.add(jsonMap);
        }
        return source;
    }

}
