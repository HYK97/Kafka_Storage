package org.example.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * packageName :  org.example.streams
 * fileName : SimpleStreamsFilter
 * author :  ddh96
 * date : 2023-04-25
 * description : kafka stream 및 filter 관련 예제
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-25                ddh96             최초 생성
 */
public class SimpleStreamsFilter {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStreamsFilter.class.getName());
    private static final String APPLICATION_NAME = "streams-application";
    // 원본 토픽
    private static final String LOG_TOPIC = "streams_log";
    // 복사 토픽
    private static final String LOG_TOPIC_COPY = "streams_log_copy";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> streamLog = streamsBuilder.stream(LOG_TOPIC);
        // filter value의 길이가 5보다 큰 메세지만 복사 토픽으로 전송
        KStream<String, String> filterStream = streamLog.filter(
            (key, value) -> value.length() > 5
        );
        filterStream.to(LOG_TOPIC_COPY);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        //close x
    }
}
