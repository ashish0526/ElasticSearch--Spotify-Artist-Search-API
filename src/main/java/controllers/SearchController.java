package controllers;


import lombok.RequiredArgsConstructor;
import model.ArtistDocument;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import service.ElasticSearchService;

import java.util.List;

@RestController
@RequestMapping("/search")
@RequiredArgsConstructor
public class SearchController {

    private final ElasticSearchService elasticSearchService;

    @GetMapping("/artist")
    public List<ArtistDocument> searchArtist(@RequestParam(name = "q", required = true) String queryString,
                                             @RequestParam(name = "userId", required = false) String userId,
                                             @RequestParam(name = "includeRanking", required = false) boolean includeRanking,
                                             @RequestParam(name = "includeUserProfile", required = false) boolean includeuserProfile,
                                             @RequestParam(name = "from", required = false, defaultValue = "0") Integer from,
                                             @RequestParam(name = "to", required = false, defaultValue =  "10") Integer to
                                             ) {

        return elasticSearchService.searchArtists(queryString, userId, includeRanking, includeuserProfile, from, to);
    }

}
