package com.example.callbackProducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 비동기로 결과를 확인할 수 있도록 Callback 인터페이스를 제공한다.
 * -> 사용자 정의 Callback 클래스를 생성하자.
 */
public class ProducerCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    // 레코드의 비동기 결과를 받아온다.
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            logger.error(e.getMessage(), e);
        else
            logger.info("RecordMetadata Sync Result === " + recordMetadata.toString());
    }
}
