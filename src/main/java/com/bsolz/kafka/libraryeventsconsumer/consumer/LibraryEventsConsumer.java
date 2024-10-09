package com.bsolz.kafka.libraryeventsconsumer.consumer;

import com.bsolz.kafka.libraryeventsconsumer.services.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumer implements AcknowledgingMessageListener<Integer, String> {

    /*@KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("Consumer Record {}", consumerRecord);
    }*/

    private final LibraryEventService eventService;
    @Override
    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Consumer Record {}", consumerRecord);
        assert acknowledgment != null;
        acknowledgment.acknowledge();
        try {
            eventService.processLibraryEvent(consumerRecord);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
