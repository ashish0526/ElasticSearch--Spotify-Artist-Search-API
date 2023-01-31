package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AggregatedUserArtistsRanking {

    private Map<String, Long> artistRankingMap;

    private Map<String, Set<ArtistRanking>> userIdToArtistRankingMap;

}
