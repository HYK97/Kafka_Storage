package org.example.producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * packageName :  org.example.producer
 * fileName : MyParitioner
 * author :  ddh96
 * date : 2023-04-20 
 * description : 파티셔너를 직접 구현하는 방법 -> 파티셔너를 직접 구현하면 키를 기준으로 파티션을 지정할 수 있다.
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-04-20                ddh96             최초 생성
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (keyBytes== null) {
            throw new IllegalArgumentException("키를 입력해주세요");
        }
        if (key.equals("myKey")) {
            return 2;
        }
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionInfos.size();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
