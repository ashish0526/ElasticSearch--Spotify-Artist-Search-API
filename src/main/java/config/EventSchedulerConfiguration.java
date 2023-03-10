package config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import service.EventProcessingService;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class EventSchedulerConfiguration {

    private final EventProcessingService eventProcessingService;

    @Scheduled(cron = "${listen-event.scheduler.cron}")
    public void runPartialIndexes() {
        eventProcessingService.updateArtistRanking();
    }

}
