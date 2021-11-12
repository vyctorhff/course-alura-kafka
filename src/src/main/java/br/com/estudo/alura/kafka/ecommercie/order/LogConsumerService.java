package br.com.estudo.alura.kafka.ecommercie.order;

import br.com.estudo.alura.kafka.ecommercie.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;

public class LogConsumerService {

    public static void main(String[] args) {
        var consume = Config.getConsumer(LogConsumerService.class);
        consume.subscribe(Arrays.asList(Config.TOPIC_NEW_ORDER, Config.TOPIC_NEW_ORDER_EMAIL));

        while (true) {
            var records = consume.poll(Duration.ofMillis(200));

            for (var record : records) {
                System.out.println("loggind: " + record.value());
            }
        }
    }
}
