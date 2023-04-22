package org.example.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
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
 * description : async callback producer  -> 비동기 콜백
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */
public class ASynchronousCallbackProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer2.class.getName());
    private static final String TOPIC = "hello.kafka.2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

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
        // 비동기 callback -> send() 메서드가 끝나지 않고 바로 다음 코드로 넘어감 논블로킹
        // 전송순서가 보장되지 않음 -> 순서가 중요한 경우에는 동기 콜백을 사용해야 함
        // 다음 보낼 데이터에 대한 응답을 받기 전에 다음 데이터를 보내는 경우가 발생할 수 있음
        RecordMetadata recordMetadata = producer.send(record,new ProducerCallback()).get();
        logger.info("{}",recordMetadata);
        producer.flush();
        producer.close();
    }
    public static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                logger.info("{}", recordMetadata);
            } else {
                logger.error("{}", e);
            }
        }
    }
}
