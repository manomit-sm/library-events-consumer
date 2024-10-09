package com.bsolz.kafka.libraryeventsconsumer.config;

import com.bsolz.kafka.libraryeventsconsumer.services.LibraryEventService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
// @EnableKafka // older version of kafka
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final LibraryEventService libraryEventService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 2);

        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L);
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setMultiplier(2_000L);
        // var errorHandler = new DefaultErrorHandler(fixedBackOff);
        var errorHandler = new DefaultErrorHandler(
                // publishingRecoverer(),
                consumerRecordRecoverer,
                exponentialBackOff
        );
        var ignoredExceptions = List.of(IllegalArgumentException.class);
        ignoredExceptions.forEach(errorHandler::addNotRetryableExceptions);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            // log the record and deliveryAttempt
        });
        return errorHandler;
    }

    private DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
           if (e.getCause() instanceof RecoverableDataAccessException) {
               return new TopicPartition("library-events.RETRY", r.partition());
           } else {
               return new TopicPartition("library-events.DLT", r.partition());
           }
        });
    }

    private final ConsumerRecordRecoverer consumerRecordRecoverer = (r, e) -> {
        var record = (ConsumerRecord<Integer, String>)r;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            libraryEventService.saveFailRecord(record, e, "RETRY");
        } else {
            libraryEventService.saveFailRecord(record, e, "DLT");
        }
    };
}
