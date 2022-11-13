package com.rcm.desenvolvimento.listeners;

import com.rcm.desenvolvimento.custom.StrConsumerCustomListener;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class StrConsumerListener {

    //@KafkaListener(groupId = "group-0", topics = "str-topic", containerFactory = "strConteinerFactory")
//    @KafkaListener(groupId = "group-0",
//            topicPartitions = {
//                @TopicPartition(topic = "str-topic", partitions = {"0"})
//            }
//            , containerFactory = "strConteinerFactory")
    @StrConsumerCustomListener(groupId = "group-1")
    public void create(String message) {
        log.info("CREATE ::: Receive message {}", message);
        throw new IllegalArgumentException("EXCEPTION... ");
    }

    //    @KafkaListener(groupId = "group-1",
//            topicPartitions = {
//                    @TopicPartition(topic = "str-topic", partitions = {"1"})
//            }
//            , containerFactory = "strConteinerFactory")
    @StrConsumerCustomListener(groupId = "group-1")
    public void log(String message) {
        log.info("LOG ::: Receive message {}", message);
    }

    //@KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strConteinerFactory")
    //@StrConsumerCustomListener(groupId = "group-2")
    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "validMessageContainerFactory")
    public void history(String message) {
        log.info("HISTORY ::: Receive message {}", message);
    }
}
