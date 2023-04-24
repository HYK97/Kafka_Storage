package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;


/**
 * packageName :  org.example.consumer
 * fileName : RebalancedListenerAndConsumer
 * author :  ddh96
 * date : 2023-04-22
 * description : rebalanced listener 사용해서 컨슈머가 리밸런싱 될 때마다 오프셋을 커밋
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-22                ddh96             최초 생성
 */
public class RebalancedListenerAndConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RebalancedListenerAndConsumer.class);
    private static final String TOPIC = "hello.kafka.2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";


    private static KafkaConsumer<String, String> consumer;
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC), new RebalancedListener());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffset.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                consumer.commitSync(currentOffset);
            }
        }
    }
    static class RebalancedListener implements ConsumerRebalanceListener {
        private static final Logger logger = LoggerFactory.getLogger(RebalancedListener.class);

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked : " + partitions.toString());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned : " + partitions.toString());
            consumer.commitSync();
            consumer.commitSync(currentOffset);
        }
    }
}