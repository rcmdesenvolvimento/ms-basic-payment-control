package com.rcm.desenvolvimento.paymentservice.service.impl;

import com.rcm.desenvolvimento.paymentservice.model.Payment;
import com.rcm.desenvolvimento.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.Serializable;

@RequiredArgsConstructor
@Log4j2
@Service
public class PaymentServiceImpl implements PaymentService {

    private final KafkaTemplate<String, Serializable> kafkaTemplate;
    @SneakyThrows // Trata as excessões
    @Override
    public void sendPayment(Payment payment) {
        log.info("Recebi o pagamento {} ", payment);
        Thread.sleep(1000); // 1 segundo
        log.info("Enviando pagamento {} ", payment);

        kafkaTemplate.send("payment-topic", payment); // envia a mensagem


    }
}
