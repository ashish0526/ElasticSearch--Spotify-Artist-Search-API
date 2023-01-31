package service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.Constants;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import model.ListenEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@AllArgsConstructor
@Slf4j
public class EventReceiverService {

    private final EventProcessingService eventProcessingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = Constants.LISTEN_EVENT_TOPIC_NAME)
    public void listen(String message) {
        if(!StringUtils.hasText(message)) {
            return;
        }
        log.info("Received message from topic: {}, message: {}", Constants.LISTEN_EVENT_TOPIC_NAME, message);

        try {
            ListenEvent listenEvent = objectMapper.readValue(message, ListenEvent.class);
            eventProcessingService.saveListenEvent(listenEvent)
        } catch (JsonProcessingException joe) {
            log.error("json processing exception occured");
        }
    }
}
