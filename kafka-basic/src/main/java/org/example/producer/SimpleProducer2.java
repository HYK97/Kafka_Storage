package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.producer
 * fileName : SimpleProducer
 * author :  ddh96
 * date : 2023-04-20 
 * description :
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */
public class SimpleProducer2 {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer2.class.getName());
    private static final String TOPIC = "hello.kafka.2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String message = "asdasd!";
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC, "myKey",message);
        producer.send(record);
        logger.info("{}",record);
        producer.flush();
        producer.close();
    }
}
