package com.example.simplekafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * commitAsync() 메서드를 호출하여 비동기 오프셋 커밋을 사용한다.
 */
public class ConsumerWithAsyncCommitTests {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithAsyncCommitTests.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
            }
            consumer.commitAsync(new OffsetCommitCallback() {   // Callback Interface
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {   // 비동기로 받은 커밋 응답 확인
                    if (e != null) {
                        System.err.println("Commit failed");
                    } else {    // 정상적으로 커밋 되었다면 Exception e 변수는 null이다.
                        System.out.println("Commit succeeded for offsets " + offsets);
                    }
                    if (e != null) {
                        logger.error("Commit failed for offsets {}", offsets, e);
                    }
                }
            });
        }
    }
}
