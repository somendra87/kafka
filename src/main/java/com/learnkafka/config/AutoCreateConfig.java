package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * @author somendraprakash created on 17/12/21
 */
@Configuration
//@Profile("local")
public class AutoCreateConfig
{

    @Bean
    public NewTopic libraryEvents(){
        return TopicBuilder.name("library-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
