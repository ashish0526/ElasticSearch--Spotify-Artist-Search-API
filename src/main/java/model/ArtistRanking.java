package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.apache.kafka.common.protocol.types.Field;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ArtistRanking {

    @JsonProperty("artist_id")
    private String artistId;

    @JsonProperty("ranking")
    @EqualsAndHashCode.Exclude
    private Long ranking;
}
