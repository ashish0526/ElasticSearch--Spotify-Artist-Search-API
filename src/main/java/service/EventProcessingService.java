package service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import model.AggregatedUserArtistsRanking;
import model.ArtistRanking;
import model.ListenEvent;
import model.UserProfile;
import org.apache.catalina.User;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static constants.Constants.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventProcessingService {

    private final RestHighLevelClient client;
    private final ElasticSearchService elasticSearchService;

    @Value("${listen-event.index.duration.inmins}")
    public int listenEventIndexDurationInMins;

    @Value("${artist-ranking.index.duration.inmins}")
    public int artistEventIndexDurationInMins;

    public void updateArtistRanking() {
        AggregatedUserArtistsRanking aggregatedUserArtistsRankings = queryRecentAggregatedArtistRankingFromListenEvents();
        Map<String, Long> artistRankingMap = aggregatedUserArtistsRankings.getArtistRankingMap();
        Map<String, Set<ArtistRanking>> userArtistRankingMap = aggregatedUserArtistsRankings.getUserIdToArtistRankingMap();

        BulkRequest bulkUpdateRankingRequest = new BulkRequest();

        //update artist rankings
        for(String artistId: artistRankingMap) {
            Map<String, Object> parameters = Collections.singletonMap("count", artistRankingMap.get(artistId));
            Script inline = new Script(ScriptType.INLINE, "painless",
                    "if (ctx._source.ranking == null) { ctx._source.ranking = params.count } else { ctx._source.ranking += params.count }", parameters);

            //update artist ranking in content index
            UpdateRequest updateArtistRankingRequest = new UpdateRequest(CONTENT_INDEX_NAME, artistId);
            updateArtistRankingRequest.script(inline);
            bulkUpdateRankingRequest.add(updateArtistRankingRequest);

            //upsert ArtistRanking document in current daily historical artist ranking index
            String currentDailyArtistRankingIndexName = getCurrentIndexName(ARTIST_RANKING_INDEX_NAME_PREFIX, artistEventIndexDurationInMins);
            ArtistRanking artistRanking = elasticSearchService.getDocument(currentDailyArtistRankingIndexName, artistId, ArtistRanking.class);
            if(artistRanking == null) {
                artistRanking = ArtistRanking.builder()
                        .artistId(artistId)
                        .ranking(artistRankingMap.get(artistId))
                        .build();
                IndexRequest artistRankingIndexRequest = new IndexRequest(currentDailyArtistRankingIndexName);
                artistRankingIndexRequest.id(artistId);
                artistRankingIndexRequest.source(elasticSearchService.toJsonString(artistRanking), XContentType.JSON);
                bulkUpdateRankingRequest.add(artistRankingIndexRequest);
            } else {
                UpdateRequest updateDailyArtistIndexRequest = new UpdateRequest(currentDailyArtistRankingIndexName, artistId);
                updateDailyArtistIndexRequest.script(inline);
                bulkUpdateRankingRequest.add(updateDailyArtistIndexRequest);
            }
        }

        //update user Artist rankings
        for(String userId : userArtistRankingMap.keySet()) {
            Set<ArtistRanking> userArtistRankingSet = userArtistRankingMap.get(userId);

            UserProfile userProfile = elasticSearchService.getDocument(USER_PROFILE_INDEX_NAME, userId, UserProfile.class);

            //if new user index the document
            if(userProfile == null) {
                userProfile = UserProfile.builder()
                        .userId(userId)
                        .artistRankingSet(userArtistRankingSet)
                        .build();
            } else {
                if(userProfile.getArtistRankingSet() == null) {
                    userProfile.setArtistRankingSet(new HashSet<>());
                }

                //update existing user profile, this part should be definetly refactored

                for(ArtistRanking artistRanking: userArtistRankingSet) {
                    ArtistRanking exisArtistRanking = userProfile.getArtistRankingSet().stream()
                            .filter(artistRanking1 -> artistRanking1.getArtistId().equals(artistRanking.getArtistId()))
                            .findFirst()
                            .orElseGet(() -> null);
                    if(exisArtistRanking == null) {
                        userProfile.getArtistRankingSet().add(artistRanking);
                    }
                    else {
                        long updateRanking = exisArtistRanking.getRanking() ==  null? 0 : exisArtistRanking.getRanking();
                        updateRanking += artistRanking.getRanking();

                        exisArtistRanking.setRanking(updateRanking);
                    }
                }

                IndexRequest userProfIndexRequest = new IndexRequest(USER_PROFILE_INDEX_NAME);
                userProfIndexRequest.id(userId);
                userProfIndexRequest.source(elasticSearchService.toJsonString(userProfile), XContentType.JSON);
                bulkUpdateRankingRequest.add(userProfIndexRequest);
            }
            if(bulkUpdateRankingRequest.numberOfActions() > 0) {
                elasticSearchService.executeBulkRequest(bulkUpdateRankingRequest);
            }
        }
    }

    private String getCurrentIndexName(String indexPrefix, int indexDurationInMins) {
        return  getIndexName(indexPrefix, indexDurationInMins, LocalDateTime.now());
    }

    private AggregatedUserArtistsRanking queryRecentAggregatedArtistRankingFromListenEvents() {
        String indexName = getPreviousIndexName(LISTEN_INDEX_EVENT_NAME_PREFIX, listenEventIndexDurationInMins);
        log.info("quering recent artist rankings from index: [{}]", indexName);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(queryBuilder);

        searchSourceBuilder.aggregation(AggregationBuilders.terms("users")
                .field("artist_id.keyword")
                .size(100)
        );

        searchSourceBuilder.aggregation(AggregationBuilders.terms("users")
                .field("user_id.keyword")
                .size(1000)
                .subAggregation(AggregationBuilders.terms("artist_rankings")
                        .field("artist_id.keyword")
                        .size(1000))
        );

        searchRequest.source(searchSourceBuilder);

        final Map<String, Long> artistRankingMap = new HashMap<>();
        final Map<String, Set<ArtistRanking>> userArtistRankingMap = new HashMap<>();

        final AggregatedUserArtistsRanking aggregatedUserArtistsRanking = AggregatedUserArtistsRanking
                .builder()
                .artistRankingMap(artistRankingMap)
                .userIdToArtistRankingMap(userArtistRankingMap)
                .build();

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            if(searchResponse.getAggregations() != null) {
                Terms artistRankingTerms = searchResponse.getAggregations().get("artist_rankings");

                artistRankingTerms.getBuckets().stream()
                        .map(bucket -> (Terms.Bucket) bucket)
                        .forEach(termsBucket -> artistRankingMap.put(termsBucket.getKeyAsString(), termsBucket.getDocCount()));

                Terms userTerms = searchResponse.getAggregations().get("users");
                userTerms.getBuckets().stream()
                        .map(bucket -> (Terms.Bucket) bucket)
                        .forEach(userBucket -> {
                            String userId = userBucket.getKeyAsString();
                            final Set<ArtistRanking> userArtistRankingSet = new HashSet<>();

                            Terms userArtistRankingTerms = userBucket.getAggregations().get("artist_rankings");
                            userArtistRankingTerms.getBuckets().stream()
                                    .map(artistRankingBucket -> (Terms.Bucket) artistRankingBucket)
                                    .forEach(artistRankingBucket -> userArtistRankingSet.add(ArtistRanking.builder()
                                            .artistId(artistRankingBucket.getKeyAsString()))
                                            .ranking(artistRankingBucket.getDocCount())
                                            .build()
                                    );
                            userArtistRankingMap.put(userId, userArtistRankingSet);

                        });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return aggregatedUserArtistsRanking;

    }

    private String getPreviousIndexName(String indexPrefix, int listenEventIndexDurationInMins) {
        return getIndexName(indexPrefix, listenEventIndexDurationInMins, LocalDateTime.now().minus(Duration.ofMinutes(listenEventIndexDurationInMins)));

    }

    private String getIndexName(String indexPrefix, int listenEventIndexDurationInMins, LocalDateTime timeStamp) {
        long instantSeconds = timeStamp.atZone(ZoneId.systemDefault()).toEpochSecond();
        long instantMinutes = instantSeconds / 60 ;
        long indexMinutes = (instantMinutes / listenEventIndexDurationInMins) * listenEventIndexDurationInMins;

        timeStamp= timeStamp.minusMinutes(instantMinutes - indexMinutes);

        String indexName = indexPrefix + timeStamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"));
        return indexName;
    }


    public IndexResponse saveListenEvent(ListenEvent listenEvent) {
        String index_name = getCurrentIndexName(LISTEN_INDEX_EVENT_NAME_PREFIX, listenEventIndexDurationInMins);
        elasticSearchService.indexDocument(index_name, null, listenEvent);
    }
}
