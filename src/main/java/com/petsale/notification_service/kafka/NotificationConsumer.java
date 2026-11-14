package com.petsale.notification_service.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final JavaMailSender mailSender;

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void consumeOrderEvent(String message) {
        log.info("📧 Received event: {}", message);

        try {
            // Simple parsing for the demo
            String orderId = message.split("\"orderId\": ")[1].split(",")[0];

            SimpleMailMessage mail = new SimpleMailMessage();
            mail.setTo("customer@example.com"); // Hardcoded for testing
            mail.setSubject("Your Pet Shop Order #" + orderId);
            mail.setText("Thank you for your purchase! \n\nEvent details:\n" + message);

            mailSender.send(mail);
            log.info("✅ Email successfully sent to Mailtrap!");

        } catch (Exception e) {
            log.error("❌ Failed to send email", e);
        }
    }
}