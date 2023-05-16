package com.opensearch.awsopensearch.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "open-search.type", havingValue = "on-prem")
public class OnPremSearchClientConfig {

    Logger log = LoggerFactory.getLogger(OnPremSearchClientConfig.class);

    @Value("${open-search.hostName}")
    private String hostName;

    @Value("${open-search.port}")
    private int port;

    @Value("${open-search.sslEnabled}")
    private boolean isSslEnabled;

    @Value("${open-search.userName}")
    private String userName;

    @Value("${open-search.password}")
    private String password;

    @Bean
    public OpenSearchClient openSearchClient() {
        RestClient restClient;
        String scheme = isSslEnabled ? "https" : "http";
        final HttpHost host = new HttpHost(hostName, port, scheme);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            log.error("Connecting to open search with credentials");
            credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(userName, password));
            restClient = RestClient.builder(host).setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)
                    )
                    .build();
        } else {
            log.error("Connecting to open search without credentials");
            restClient = RestClient.builder(host).build();
        }
        final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new OpenSearchClient(transport);
    }
}
