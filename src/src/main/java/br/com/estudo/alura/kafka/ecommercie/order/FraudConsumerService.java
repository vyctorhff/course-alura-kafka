package br.com.estudo.alura.kafka.ecommercie.order;

import br.com.estudo.alura.kafka.ecommercie.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;

public class FraudConsumerService {

    public static void main(String[] args) {
        var consumer = Config.getConsumer(FraudConsumerService.class);
        consumer.subscribe(List.of(Config.TOPIC_NEW_ORDER));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

            for (var record: records) {
                System.out.println("Processing order: key" + record.key() + " value: " + record.value());

                try {
                    Thread.sleep(2000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
