package com.opensearch.awsopensearch.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.core.net.ssl.SslConfigurationException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "open-search.type", havingValue = "aws")
public class AwsOpenSearchClientConfig {

    @Value("${open-search.hostName}")
    private String hostname;

    @Value("${open-search.sslEnabled}")
    private boolean isSslEnabled;

    @Value("${open-search.userName:}")
    private String username;

    @Value("${open-search.password:}")
    private String password;

    @Value("${open-search.port}")
    private int port;

    @Bean
    public RestHighLevelClient restHighLevelClient() throws SslConfigurationException {
        String scheme = isSslEnabled ? "https" : "http";
        if (scheme.equalsIgnoreCase("http")) {
            throw new SslConfigurationException(new Exception("sslEnabled property should be true!"));
        }
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, scheme))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider)
                );
        return new RestHighLevelClient(builder);
    }
}
