package com.petsale.notification_service.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class NotificationConsumer {

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void consumeOrderEvent(String message) {
        log.info("==================================================");
        log.info("📧 NOTIFICATION SERVICE RECEIVED NEW ORDER EVENT");
        log.info("Payload: {}", message);
        log.info("Simulating sending email to customer...");
        log.info("==================================================");
    }
}