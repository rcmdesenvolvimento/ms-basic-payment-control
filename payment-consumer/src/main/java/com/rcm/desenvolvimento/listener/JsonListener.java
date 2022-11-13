package com.rcm.desenvolvimento.listener;

import com.rcm.desenvolvimento.model.Payment;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class JsonListener {

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "create-group", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void antiFraud(@Payload Payment payment) {
        log.info("Recebi o pagamento {}", payment.toString());
        Thread.sleep(2000);

        log.info("Validando fraude...");
        Thread.sleep(2000);

        log.info("Compra aprovada...");
        Thread.sleep(3000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "pdf-group", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void pdfGenerator(@Payload Payment payment) {
        Thread.sleep(4000);
        log.info("Gerando PDF do produto {} ", payment.toString());
        Thread.sleep(4000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "email-group", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void sendEmail() {
        Thread.sleep(3000);
        log.info("Enviando e-mail de confirmação...");
    }

}
