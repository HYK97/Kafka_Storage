
package org.example.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.consumer
 * fileName : ASynchronousCommitConsumer
 * author :  ddh96
 * date : 2023-04-20 
 * description : 동기 커밋
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */

public class ASynchronousCommitConsumer {
    private static final Logger logger = LoggerFactory.getLogger(org.example.producer.SimpleProducer.class.getName());
    private static final String TOPIC = "hello.kafka.2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 항상 필요한 설정 3가지
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
            //consumer.commitAsync();
            //콜백을 받고 싶을때 OffsetCommitCallback을 구현
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    logger.error("Commit failed for offsets {}", offsets, exception);
                }else {
                    logger.info("Commit success for offsets {}", offsets);
                }
            });
        }
    }
}


