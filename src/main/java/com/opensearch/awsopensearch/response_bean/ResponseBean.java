package com.opensearch.awsopensearch.response_bean;

import lombok.*;

import java.util.List;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class ResponseBean {
    private List<Map<String,Object>> results;
    private String index;
}
