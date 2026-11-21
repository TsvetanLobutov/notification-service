package com.petsale.notification_service.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import jakarta.mail.internet.MimeMessage;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final JavaMailSender mailSender;
    private final ObjectMapper objectMapper;
    private final TemplateEngine templateEngine; // Inject Thymeleaf

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void consumeOrderEvent(String message) {
        try {
            log.info("📧 Received event: {}", message);

            JsonNode jsonNode = objectMapper.readTree(message);
            String orderId = jsonNode.get("orderId").asText();
            String productId = jsonNode.get("productId").asText();
            String quantity = jsonNode.get("quantity").asText();
            String status = jsonNode.get("status").asText();

            // 1. Prepare the Thymeleaf context (the variables for the HTML)
            Context context = new Context();
            context.setVariable("orderId", orderId);
            context.setVariable("productId", productId);
            context.setVariable("quantity", quantity);
            context.setVariable("status", status);

            // 2. Process the HTML template
            String htmlContent = templateEngine.process("order-confirmation", context);

            // 3. Send the email
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");

            helper.setTo("customer@example.com");
            helper.setSubject("🐾 Your Pet Shop Order #" + orderId + " Confirmation");
            helper.setText(htmlContent, true); // true = this is HTML!

            mailSender.send(mimeMessage);
            log.info("✅ Email successfully sent to Mailtrap!");

        } catch (Exception e) {
            log.error("❌ Failed to send email", e);
        }
    }
}