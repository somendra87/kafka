package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author somendraprakash created on 17/12/21
 */
@RestController
@Slf4j
public class LibraryEventsController
{
    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException {
        //invoke kafka producer
        log.info("Before sendLibraryEvent");
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        //SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventsSynchronous(libraryEvent);
        //log.info("SendResult is : {}", sendResult.toString());
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventApproach2(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent){
        return null;
    }
}
