package org.example.cunsumer;

import java.time.Duration;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.cunsumer
 * fileName : SimpleConsumer
 * author :  ddh96
 * date : 2023-04-20 
 * description :
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */

public class SimpleConsumer {
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
        // enable.auto.commit 옵션 설정 (오프셋 자동 커밋)
        // true : 컨슈머가 오프셋을 자동으로 커밋 (기본값) -> 중복 데이터 발생 가능성 있음
        // false : 컨슈머가 오프셋을 수동으로 커밋 -> 중복 데이터 발생 가능성 없음
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // auto.offset.reset 옵션 설정 (오프셋 자동 리셋)
        // lastest :  가장 높은(가장 최근에 넣은) 오프셋부터 읽기 시작
        // earliest : 가장 낮은(가장 오래된) 오프셋부터 읽기 시작
        // none : 컨슈머 그룹이 커밋한 기록이 있는지 찾아봄  -> 기록이 없으면 에러 발생 있으면 기존 커밋 기록 이후 부터 읽기 시작
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "lastest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));

        // consumer.poll(Duration.ofSeconds(1)); 컨슈머 버퍼에 데이터가 없으면 1초동안 대기 타임아웃
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}


