package br.com.estudo.alura.kafka.ecommercie.hello;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class HelloKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloKafkaConsumer.class);

    public static final String HOST = "localhost:9092";

    public static Properties getProperties() {
        var props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, HelloKafkaConsumer.class.getName());
        return props;
    }

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Arrays.asList(HelloKafkaProducer.TOPIC));

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
