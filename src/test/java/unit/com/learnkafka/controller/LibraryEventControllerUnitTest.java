package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryBook;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author somendraprakash created on 17/12/21
 */
@WebMvcTest(LibraryEventsController.class)//handy for controller layer
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest
{
    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper mapper = new ObjectMapper();

    @Test
    void postLibraryEvent() throws Exception {
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
        String json = mapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendLibraryEventApproach2(isA(LibraryEvent.class));

        //when
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isCreated());

        //then
    }
}
