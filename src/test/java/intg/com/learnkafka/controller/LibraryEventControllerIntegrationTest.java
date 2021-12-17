package com.learnkafka.controller;

import com.learnkafka.domain.LibraryBook;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author somendraprakash created on 17/12/21
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
})
public class LibraryEventControllerIntegrationTest
{
    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> kafkaConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>(KafkaTestUtils.consumerProps
                ("group1", "true", embeddedKafkaBroker));
        kafkaConsumer = new DefaultKafkaConsumerFactory<Integer, String>(
                configMap, new IntegerDeserializer(), new StringDeserializer()
        ).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(kafkaConsumer);
    }

    @AfterEach
    void tearDown() {
        kafkaConsumer.close();
    }

    @Test
    void postLibraryEvent() {

        //given
        LibraryBook book = LibraryBook.builder()
                .bookId(889224)
                .bookName("New Book On Data Structures")
                .bookAuthor("Somendra Prakash")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .libraryEventType(LibraryEventType.NEW)
                .build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Conent-Type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        //when
        ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/v1/libraryevent"
                , HttpMethod.POST
                , request
                , LibraryEvent.class
        );

        //then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, "library" +
                "-events");
        String expectedRecord = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":889224," +
                "\"bookName\":\"New Book On Data Structures\",\"bookAuthor\":\"Somendra Prakash\"}}";
        String value = consumerRecord.value();

        assertEquals(expectedRecord, value);
    }
}
