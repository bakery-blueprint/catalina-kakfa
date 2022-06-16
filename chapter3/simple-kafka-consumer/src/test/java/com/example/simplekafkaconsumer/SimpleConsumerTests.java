package com.example.simplekafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

/**
 * 카프카 컨슈머 프로젝트를 생성하여 레코드를 처리한다.
 */
public class SimpleConsumerTests {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumerTests.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";    // 컨슈머 그룹 지정 (컨슈머 목적 구분)

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // KafkaConsumer 인스턴스를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // subscribe(): 컨슈머에게 토픽을 할당한다. (컨슈머 그룹 선언 필요)
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        // 컨슈머에 할당된 파티션 확인
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();
        logger.info("assignedTopicPartition: {}", assignedTopicPartition);

        while (true) {  // 지속적으로 반복 호출 하기 위함.
            // poll(): 컨슈머가 데이터를 가져와 처리한다.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
            }
        }
    }
}
