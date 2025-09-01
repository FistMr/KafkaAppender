package com.puchkov.kafkalog4j2test.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);

    @GetMapping("/test")
    public String testLogging() {
        logger.info("Тестовое информационное сообщение");
        logger.warn("Тестовое предупреждение");
        logger.error("Тестовая ошибка");

        return "Логи отправлены в Kafka! Проверьте топик 'test-logs'";
    }
}
