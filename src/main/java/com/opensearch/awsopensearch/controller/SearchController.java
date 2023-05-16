package com.opensearch.awsopensearch.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensearch.awsopensearch.response_bean.ResponseBean;
import com.opensearch.awsopensearch.service.SearchFilterInterface;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/aws-open-search")
public class SearchController {
    private final SearchFilterInterface searchFilterInterface;
    private final ObjectMapper objectMapper;

    public SearchController(SearchFilterInterface searchFilterInterface, ObjectMapper objectMapper) {
        this.searchFilterInterface = searchFilterInterface;
        this.objectMapper = objectMapper;
    }

    @PostMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseBean ingestData(@RequestParam(name = "index", required = false) String index,
                                                  @RequestParam(name = "id") Optional<Integer> id,
                                                  @RequestParam(name = "limit") Optional<Integer> limit,
                                                  @RequestBody Optional<Map<String,Object>> data) throws Exception {
        return searchFilterInterface.ingestData(index, data, id, limit);
    }

    @GetMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseBean fetchData(@RequestParam(name = "index") String index,
                               @RequestParam(name = "id") Optional<String> id,
                               @RequestParam(name = "isScroll") Optional<Boolean> isScroll) throws Exception {
        return searchFilterInterface.fetchData(index, id, isScroll);
    }

    @DeleteMapping
    public void deleteData(@RequestParam(name = "index") String index) {
        searchFilterInterface.deleteData(index);
    }

}