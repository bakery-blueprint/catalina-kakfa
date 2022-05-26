package com.example.simplekafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 카프카 프로듀서 프로젝트를 생성하여 레코드를 전송한다.
 */
public class SimpleProducerTests {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducerTests.class);
    private final static String TOPIC_NAME = "test";                    // 프로듀서가 전송할 토픽 이름
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";    // 서버의 host, IP 접속 정보

    public static void main(String[] args) {

        // 프로듀서 옵션들을 key/value 값으로 선언한다.
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());      // 메시지 키, 값을 직렬화하기 위한 직렬화 클래스 (String 객체 전송)
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 인스턴스를 생성하여 ProducerRecord를 전송할때 사용한다.
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";

        // 카프카 브로커로 데이터를 보내기 위해 ProducerRecord 를 생성한다.
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue); // 메시지 키, 값의 타입은 직렬화 클래스와 동일하게 설정해야 한다.

        // send(): 즉각적인 전송은 아니고, record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다. (배치 전송)
        producer.send(record);
        logger.info("{}", record);

        // 배치 전송을 테스트 하기 위하여 record 한개 더 생성
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "testMessage2");
        producer.send(record2);

        // flush(): 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송한다.
        producer.flush();

        // producer 인스턴스의 리소스 종료
        producer.close();
    }
}
