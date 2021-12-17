package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author somendraprakash created on 17/12/21
 */
@Component
@Slf4j
public class LibraryEventProducer
{
    private String topic = "library-events";

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture =
                kafkaTemplate.sendDefault(key, value);
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>()
        {
            @Override
            public void onFailure(Throwable exception) {
                handleFailure(key, value, exception);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    public SendResult<Integer, String> sendLibraryEventsSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        }
        catch (InterruptedException | ExecutionException exception) {
            log.error("ExecutionException/InterruptedException sending the message and exception is : {}",
                    exception.getMessage());
            throw exception;
        }
        catch (Exception exception) {
            log.error("Exception sending the message and exception is : {}",
                    exception.getMessage());
            throw exception;
        }
        return sendResult;
    }

    public void sendLibraryEventApproach2(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture =
                kafkaTemplate.send(producerRecord);

        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>()
        {
            @Override
            public void onFailure(Throwable exception) {
                handleFailure(key, value, exception);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        // adding headers to producer record
        // headers are iterable so we can create a list of header
        List<Header> recordHeaders = List.of(new RecordHeader("event-source",
                "scanner".getBytes(StandardCharsets.UTF_8)));

        return new ProducerRecord<>(topic, null, key, value, null);
    }


    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is : {} ", ex.getMessage());
        try {
            throw ex;
        }
        catch (Throwable throwable) {
            log.error("Error OnFailure {}", throwable.getMessage());
            throwable.printStackTrace();
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully for the key : {} and the value is :{} and partition is : {} ", key, value
                , result.getRecordMetadata().partition());
    }
}
