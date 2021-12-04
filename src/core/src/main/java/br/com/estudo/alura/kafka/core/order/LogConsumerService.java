package br.com.estudo.alura.kafka.core.order;

import br.com.estudo.alura.kafka.common.config.Config;
import br.com.estudo.alura.kafka.common.config.ConfigConsumer;

import java.time.Duration;
import java.util.Arrays;

public class LogConsumerService {

    public static void main(String[] args) {
        var consume = ConfigConsumer.getConsumer(LogConsumerService.class);
        consume.subscribe(Arrays.asList(Config.TOPIC_NEW_ORDER, Config.TOPIC_NEW_ORDER_EMAIL));

        while (true) {
            var records = consume.poll(Duration.ofMillis(200));

            for (var record : records) {
                System.out.println("logging: " + record.value());
            }
        }
    }
}
