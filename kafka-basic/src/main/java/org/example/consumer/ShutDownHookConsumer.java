package org.example.consumer;

import java.time.Duration;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.consumer
 * fileName : ShutDownHookConsumer
 * author :  ddh96
 * date : 2023-04-23
 * description : wakeup() 메서드를 이용한 안전한 종료 방법
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */

public class ShutDownHookConsumer {
    private static final Logger logger = LoggerFactory.getLogger(org.example.producer.SimpleProducer.class.getName());
    private static final String TOPIC = "hello.kafka.2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        /*`Runtime.getRuntime().addShutdownHook(new ShutdownThread())` 코드는 자바 프로그램에서 실행되며, JVM이 종료되기 직전에 특정 작업을 수행하도록 설정하는 것입니다.
         * 이 코드는 다음과 같은 원리로 동작합니다:
         * 1. `Runtime.getRuntime()` 메서드를 호출하여 현재 실행 중인 자바 애플리케이션의 `Runtime` 인스턴스를 가져옵니다. 이 인스턴스는 애플리케이션의 실행 환경에 대한 정보와 제어를 제공합니다.
         * 2. `addShutdownHook` 메서드를 사용하여 셧다운 훅을 등록합니다. 셧다운 훅은 프로그램이 종료되기 직전에 실행되는 스레드입니다. 이 예제에서는 `new ShutdownThread()`를 인자로 전달하여, `ShutdownThread` 클래스의 인스턴스를 생성하고 그것을 셧다운 훅으로 등록합니다.
         * 3. 프로그램이 정상적으로 종료되거나 예외로 인해 종료될 때, 등록된 셧다운 훅이 실행됩니다. 셧다운 훅에 정의된 작업은 프로그램 종료 직전에 수행되며, 자원 해제, 로그 기록 등의 작업을 할 수 있습니다.
         * 4. 셧다운 훅의 실행이 완료되면 프로그램이 종료됩니다.
         * 코드에서 `new ShutdownThread()` 부분을 사용자가 원하는 작업을 수행하는 스레드로 교체하면, 해당 작업이 프로그램 종료 전에 실행됩니다.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "lastest");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("{}", record);
                }
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup exception");
        } finally {
            logger.warn("Consumer close");
            consumer.close();
        }
    }

    static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}


