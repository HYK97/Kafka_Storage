package org.example.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.producer
 * fileName : CallbackProducer
 * author :  ddh96
 * date : 2023-04-20 
 * description : sync callback producer
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */
public class SynchronousCallbackProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer2.class.getName());
    private final static String TOPIC = "hello.kafka.2";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String message = "test!";
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC, "myKey",message);
        logger.info("{}",record);
        // 동기 콜백 -> send() 메서드가 끝날 때까지 기다림 스레드 블로킹
        RecordMetadata recordMetadata = producer.send(record).get();
        logger.info("{}",recordMetadata);
        producer.flush();
        producer.close();
    }
}
