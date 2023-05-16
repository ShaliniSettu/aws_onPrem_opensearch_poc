package com.opensearch.awsopensearch.service;

import com.opensearch.awsopensearch.response_bean.ResponseBean;

import java.util.Map;
import java.util.Optional;

public interface SearchFilterInterface {
    ResponseBean ingestData(String index, Optional<Map<String, Object>> data, Optional<Integer> id, Optional<Integer> limit) throws Exception;

    ResponseBean fetchData(String index, Optional<String> id, Optional<Boolean> isScroll) throws Exception;

    void deleteData(String index);
}
