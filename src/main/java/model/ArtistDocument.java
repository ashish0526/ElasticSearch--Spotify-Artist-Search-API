package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ArtistDocument {

    @JsonProperty("artist_id")
    private String artistId;

    @JsonProperty("artist_name")
    private String artistName;

    @JsonProperty("ranking")
    private Long ranking;

    @JsonProperty("_score")
    private Float _score;
}
