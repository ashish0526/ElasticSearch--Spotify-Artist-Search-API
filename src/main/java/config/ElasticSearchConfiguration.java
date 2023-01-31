package config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.common.protocol.types.Field;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Configuration
public class ElasticSearchConfiguration {


    private List<String> elasticHosts;

    private String userName;

    private String password;

    @Bean("elasticsearchObjectMapper")
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new JavaTimeModule());

    }

    @Bean
    public RestHighLevelClient client() {
        HttpHost[] httpHostsArray = elasticHosts.stream()
                .map(this::createUrl)
                .map(u -> new HttpHost(u.getHost(), u.getPort(), u.getProtocol()))
                .toArray(HttpHost[]::new);

        SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();

        final CredentialsProvider credentialsProvider = userName ==  null ? null : BasicCredentialsProvider();

        if(credentialsProvider != null) {
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        }

        //low level rest client
        RestClientBuilder restClientBuilder = RestClient.builder(httpHostsArray)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        if(credentialsProvider != null) {
                            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    }
                })
                .setFailureListener(sniffOnFailureListener);
        //high level rest client

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // create sniffer and set to failure listener
        Sniffer sniffer = Sniffer.builder(restHighLevelClient.getLowLevelClient())
                .setSniffAfterFailureDelayMillis(30000)
                .build();
        sniffOnFailureListener.setSniffer(sniffer);

        return restHighLevelClient;
    }

    private URL createUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

}
