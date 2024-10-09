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
public class LibraryEventRetryConsumer implements AcknowledgingMessageListener<Integer, String> {

    private final LibraryEventService eventService;
    @Override
    @KafkaListener(topics = "library-events.RETRY", groupId = "retry-listener-group", autoStartup = "${retryListener.startup:true}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Consumer Record {}", consumerRecord);
        consumerRecord.headers()
                .forEach(header -> {
                    log.info("Key {}, Value {}", header.key(), header.value());
                });
        assert acknowledgment != null;
        acknowledgment.acknowledge();
    }
}
