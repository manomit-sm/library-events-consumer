package com.bsolz.kafka.libraryeventsconsumer.scheduler;

import com.bsolz.kafka.libraryeventsconsumer.entities.FailureRecord;
import com.bsolz.kafka.libraryeventsconsumer.repositories.FailureRecordRepository;
import com.bsolz.kafka.libraryeventsconsumer.services.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

    private final LibraryEventService libraryEventService;
    private final FailureRecordRepository repository;

    @Scheduled(fixedRate = 10000)
    public void retryRetryRecord() {
        repository
                .findAllByStatus("Retry")
                .forEach(failureRecord -> {
                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventService.processLibraryEvent(consumerRecord);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffsetValue(),
                failureRecord.getKeyValue(),
                failureRecord.getErrorRecord()
        );
    }
}
