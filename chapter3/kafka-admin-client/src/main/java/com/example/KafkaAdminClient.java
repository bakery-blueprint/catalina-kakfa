package com.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class KafkaAdminClient {
    private final static Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws Exception {

        // 어드민 API 선언
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        AdminClient admin = AdminClient.create(configs);

        logger.info("== Get broker information");
        for (Node node : admin.describeCluster().nodes().get()) {
            logger.info("node : {}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr)); // 브로커 정보 조회
            describeConfigs.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> logger.info(configEntry.name() + "= " + configEntry.value()));
            });
        }

        logger.info("== Get default num.partitions");
        for (Node node : admin.describeCluster().nodes().get()) {
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            Config config = describeConfigs.all().get().get(cr);
            Optional<ConfigEntry> optionalConfigEntry = config.entries().stream().filter(v -> v.name().equals("num.partitions")).findFirst();
            ConfigEntry numPartitionConfig = optionalConfigEntry.orElseThrow(Exception::new);
            logger.info("{}", numPartitionConfig.value());
        }

        logger.info("== Topic list");
        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            logger.info("{}", topicListing.toString());
        }

        logger.info("== test topic information");
        Map<String, TopicDescription> topicInformation = admin.describeTopics(Collections.singletonList("test")).all().get(); // 토픽 정보조회
        logger.info("{}", topicInformation);

        logger.info("== Consumer group list");
        ListConsumerGroupsResult listConsumerGroups = admin.listConsumerGroups();
        listConsumerGroups.all().get().forEach(v -> {
            logger.info("{}", v);
        });

        admin.close();
    }
}