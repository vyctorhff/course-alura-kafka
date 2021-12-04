package br.com.estudo.alura.kafka.hello;

import br.com.estudo.alura.kafka.common.config.Config;
import br.com.estudo.alura.kafka.common.config.ConfigConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class HelloKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloKafkaConsumer.class);

    public static void main(String[] args) {
        var consumer = ConfigConsumer.getConsumer(HelloKafkaConsumer.class);
        consumer.subscribe(Arrays.asList(Config.TOPIC_HELLO));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (var record: records) {
                LOGGER.info("-------------------------------------");
                LOGGER.info("Processing");

                LOGGER.info("Content: " + record.value());
                LOGGER.info(String.format("success - topic: %s, offset: %s, partition: %s, time: %s",
                        record.topic(),
                        record.offset(),
                        record.partition(),
                        record.timestamp()
                ));
                LOGGER.info("-------------------------------------");
            }
        }
    }
}
