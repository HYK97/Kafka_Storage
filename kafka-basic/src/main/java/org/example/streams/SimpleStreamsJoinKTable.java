package org.example.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.streams
 * fileName : SimpleStreamsJotinKTable
 * author :  ddh96
 * date : 2023-05-03 
 * description : kafka streams join 관련 예제
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-05-03                ddh96             최초 생성
 */
public class SimpleStreamsJoinKTable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStreamsFilter.class.getName());
    private static final String APPLICATION_NAME = "order-join-application";

    private static final String ORDER_STREAM = "order";
    private static final String ADDRESS_TABLE = "address";
    private static final String ORDER_JOIN_STREAM = "order_join";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> orderStream = streamsBuilder.stream(ORDER_STREAM);
        KTable<String, String> addressTable = streamsBuilder.table(ADDRESS_TABLE);

        // Kstream와 Ktable에서 동일 한 메세지 키를 가지고 있을 때 join
        //여기서는 order 토픽의 물품이름과 address 토픽의 주소를 합쳐서 order_join 토픽으로 전송
        orderStream.join(addressTable,
            (order, address) -> order + "send to " + address).to(ORDER_JOIN_STREAM);
        //ex order -> key,value "sumin" : "apple", address -> key,value "sumin" : "seoul"
        //결과 order_join -> key,value "sumin" : "apple send to seoul"

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        //close x
    }
}
