package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author somendraprakash created on 17/12/21
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent
{
    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    private LibraryBook book;
}
