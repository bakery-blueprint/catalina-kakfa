package com.example.simplekafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * 리밸런스 발생 시 데이터를 중복 처리하지 않게 하기 위해서
 * 리밸런스 리스너를 이용하여, 리밸런스 발생 시 처리한 데이터를 기준으로 커밋 한다.
 */
public class ConsumerWithRebalanceListenerTests {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithRebalanceListenerTests.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";


    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // 리밸런스 발생시 수동 커밋

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()), // key
                        new OffsetAndMetadata(record.offset() + 1, null));  // value (현재 처리한 오프셋에 1을 더한 값)
                consumer.commitSync(currentOffset); // 해당 토픽, 파티션의 오프셋이 매번 커밋된다.
            }
        }
    }

    private static class RebalanceListener implements  ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned : " + partitions.toString());
            // 리밸런스가 발생하면 가장 마지막으로 처리 완료한 레코드를 기준으로 커밋한다.
            consumer.commitSync();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked : " + partitions.toString());
        }
    }
}
