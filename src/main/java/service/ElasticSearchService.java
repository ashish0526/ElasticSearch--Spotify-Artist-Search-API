package service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import model.ArtistDocument;
import model.ArtistRanking;
import model.ListenEvent;
import model.UserProfile;
import org.apache.kafka.common.protocol.types.Field;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static constants.Constants.CONTENT_INDEX_NAME;
import static constants.Constants.USER_PROFILE_INDEX_NAME;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticSearchService {

    private final RestHighLevelClient restHighLevelClient;
    private final ObjectMapper objectMapper;
    public List<ArtistDocument> searchArtists(String queryString, String userId, boolean includeRanking, boolean includeuserProfile, Integer from, Integer size) {

        SearchRequest searchRequest = new SearchRequest(CONTENT_INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);

        //full text search query String
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                .should(new MultiMatchQueryBuilder(queryString)
                        .field("artist_name", 2.0f)
                        .field("artist_name.prefix", 1.0f)
                        .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
                        .operator(Operator.AND)
                        .fuzziness("0")
                )
                .should(new MultiMatchQueryBuilder(queryString)
                        .field("artist_name.prefix", 0.5f)
                        .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
                        .operator(Operator.AND)
                        .fuzziness("1")
                )
                .minimumShouldMatch(1);

        List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilderList = new ArrayList<>();


        //ranking based score functional list

        if (includeRanking) {
            filterFunctionBuilderList.add(
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                            ScoreFunctionBuilders.scriptFunction
                            )
                    );
        }

        //user Profile Based score function builder
        if(includeuserProfile) {
            UserProfile userProfile = getDocument(USER_PROFILE_INDEX_NAME, userId, UserProfile.class);

            if(userProfile != null && !userProfile.getArtistRankingSet().isEmpty()) {
                List<String> artistIdList = new ArrayList<>();
                Map<String, Float> artistIdTOBoostFactorMap = new HashMap<>();
                for(ArtistRanking artistRanking: userProfile.getArtistRankingSet()) {
                    artistIdList.add(artistRanking.getArtistId());
                    artistIdTOBoostFactorMap.put(artistRanking.getArtistId(), log2(artistRanking.getRanking()));
                }
                String scriptStr = "params.boost.get(doc[params.artistIdFieldName].value)";
                String artistIdFieldName = "artist_id";

                Map<String, Object> params = new HashMap<>();
                params.put("boosts", artistIdTOBoostFactorMap);
                params.put("artistIdFieldName", artistIdFieldName);

                Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);

                filterFunctionBuilderList.add(
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                new TermsQueryBuilder(artistIdFieldName, artistIdList),
                                ScoreFunctionBuilders.scriptFunction(script)
                        )
                );


            }
        }

        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuildersArray = filterFunctionBuilderList.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[filterFunctionBuilderList.size()]);

        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(boolQueryBuilder, filterFunctionBuildersArray)
                .boost(1)
                .scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                .boostMode(CombineFunction.MULTIPLY);

        searchSourceBuilder.query(functionScoreQueryBuilder);
        searchSourceBuilder.sort("_score", SortOrder.DESC);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);

        List<ArtistDocument> result = new ArrayList<>();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            result = Arrays.stream(searchResponse.getHits().getHits())
                    .map(searchHit -> {
                        ArtistDocument artistDocument = toDocumentObject(searchHit.getSourceAsString(), ArtistDocument.class);
                        artistDocument.set_score(searchHit.getScore());
                        return artistDocument;
                    })
                    .collect(Collectors.toList());
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        }

        return result;
    }

    public static float log2(float x)
    {
        return (float) (Math.log(x) / Math.log(2));
    }

    public <T> T getDocument(String indexName, String id, Class<T> clazz) {
        GetRequest getRequest = new GetRequest(indexName, id);
        GetResponse getResponse = null;

        try {
            getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ElasticsearchStatusException ex) {
            if(ex.status().equals(RestStatus.NOT_FOUND)) {
                return null;
            }
            throw ex;
        }

        T result = null;
        if(getResponse.isExists()) {
            result = toDocumentObject(getResponse.getSourceAsString(), clazz);
        }
        
        return  result;
    }

    private <T> T toDocumentObject(String jsonString, Class<T> clazz) {
        T result = null;
        try {
            result = objectMapper.readValue(jsonString, clazz);
        } catch (JsonProcessingException jsex) {
            throw new RuntimeException(jsex);
        }
        return result;
    }

    public String toJsonString(Object document) {
        String jsonString;
        try {
            jsonString = objectMapper.writeValueAsString(document);
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }

        return jsonString;
    }

    public BulkResponse executeBulkRequest(BulkRequest bulkRequest) {
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("executed " + bulkRequest.numberOfActions() + " bulk Documents.. ");

            for(BulkItemResponse bulkItemResponse: bulkResponse.getItems()) {
                if(bulkResponse.hasFailures()) {
                    log.error("\t bulk Failure {}", bulkItemResponse.getFailureMessage());
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return bulkResponse;
    }

    public IndexResponse indexDocument(String index_name, String id, Object doc) {
        IndexRequest indexRequest = new IndexRequest(index_name);

        if(id != null) {
            indexRequest.id(id);
        }

        indexRequest.source(toJsonString(doc), XContentType.JSON);

        IndexResponse indexResponse;

        try {
            indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return indexResponse;
    }


}
