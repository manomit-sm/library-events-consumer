package com.bsolz.kafka.libraryeventsconsumer.services;

import com.bsolz.kafka.libraryeventsconsumer.entities.FailureRecord;
import com.bsolz.kafka.libraryeventsconsumer.entities.LibraryEvent;
import com.bsolz.kafka.libraryeventsconsumer.repositories.FailureRecordRepository;
import com.bsolz.kafka.libraryeventsconsumer.repositories.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventService {

    private final ObjectMapper objectMapper;
    private final LibraryEventRepository repository;
    private final FailureRecordRepository failureRecordRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        var libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                saveEvent(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                saveEvent(libraryEvent);
            default:
                log.info("Invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id can not be null");
        }
        final Optional<LibraryEvent> eventOptional = repository.findById(libraryEvent.getLibraryEventId());
        if (eventOptional.isEmpty()) {
            throw new IllegalArgumentException("Not a valida library event");
        }
    }

    private void saveEvent(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
    }

    public void saveFailRecord(ConsumerRecord<Integer,String> r, Exception e, String status) {
        var failureRecord = new FailureRecord(
                null,
                r.topic(),
                r.key(),
                r.value(),
                r.partition(),
                r.offset(),
                e.getMessage(), status
        );
        failureRecordRepository.save(failureRecord);
    }
}
