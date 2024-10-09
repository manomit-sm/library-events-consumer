package com.bsolz.kafka.libraryeventsconsumer;

import com.bsolz.kafka.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.bsolz.kafka.libraryeventsconsumer.entities.LibraryEvent;
import com.bsolz.kafka.libraryeventsconsumer.repositories.LibraryEventRepository;
import com.bsolz.kafka.libraryeventsconsumer.services.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {
		"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsConsumerApplicationTests {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;

	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;

	@SpyBean
	LibraryEventService libraryEventServiceSpy;

	@Autowired
	LibraryEventRepository libraryEventRepository;

	@BeforeEach
	void setUp() {
		/* for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		} */
		final MessageListenerContainer firstContainer = endpointRegistry
				.getListenerContainers()
				.stream().filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-events-listeners-group"))
				.toList().getFirst();
		ContainerTestUtils.waitForAssignment(firstContainer, embeddedKafkaBroker.getPartitionsPerTopic());

	}

	@Test
	void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
		String jsonString = ""; // for now blank
		final SendResult<Integer, String> sendResult = kafkaTemplate.sendDefault(jsonString).get();

		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		final List<LibraryEvent> all = libraryEventRepository.findAll();
		assert all.size() == 1;
	}

}
