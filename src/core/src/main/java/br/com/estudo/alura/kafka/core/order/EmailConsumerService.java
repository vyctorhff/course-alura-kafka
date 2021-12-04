package br.com.estudo.alura.kafka.core.order;

import br.com.estudo.alura.kafka.common.config.Config;
import br.com.estudo.alura.kafka.common.config.ConfigConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;

public class EmailConsumerService {

    public static void main(String[] args) {
        var consumer = ConfigConsumer.getConsumer(EmailConsumerService.class);
        consumer.subscribe(List.of(Config.TOPIC_NEW_ORDER_EMAIL));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            for (var record : records) {
                System.out.println("Sending email to : " + record.value());
            }
        }
    }
}
