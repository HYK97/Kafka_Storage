package org.example.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * packageName :  org.example.streams
 * fileName : SimpleProcessor
 * author :  ddh96
 * date : 2023-05-04
 * description : kafka stream Global ktable join 예제
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-05-04                ddh96             최초 생성
 */
public class SimpleKafkaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStreamsFilter.class.getName());
    private static final String APPLICATION_NAME = "global-ktable-join-application";
    // 원본 토픽
    private static final String ORDER_STREAM = "order";
    private static final String ORDER_JOIN_STREAM = "order_join";
    // 복사 토픽
    private static final String ADDRESS_GLOBAL_TABLE = "address_v2";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> orderStream = streamsBuilder.stream(ORDER_STREAM);
        GlobalKTable<String, String> addressTable = streamsBuilder.globalTable(ADDRESS_GLOBAL_TABLE);

        orderStream.join(addressTable,
                (orderKey, orderValue) -> orderKey,
                (order, address) -> order + " send to " + address
        ).to(ORDER_JOIN_STREAM);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        //close x
    }
}
