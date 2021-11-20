package br.com.estudo.alura.kafka.ecommercie.config;

import br.com.estudo.alura.kafka.ecommercie.infra.GsonKafkaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Config {

    public static final String HOST = "localhost:9092";

    public static final String TOPIC_HELLO = "ECOMMERCE-HELLO";
    public static final String TOPIC_NEW_ORDER = "ECOMMERCE-NEW-ORDER";
    public static final String TOPIC_NEW_ORDER_EMAIL = "ECOMMERCE-NEW-ORDER-EMAIL";

    public static Properties getConsumerProperties(Class<?> clazz) {
        var props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clazz.getSimpleName());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, getRandomId());
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        return props;
    }

    public static Properties getProducerProperties() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);

        // tell kafka that the message will be a string
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonKafkaSerializer.class.getName());

        return props;
    }

    public static KafkaConsumer<String, String> getConsumer(Class<?> clazz) {
        return new KafkaConsumer<>(getConsumerProperties(clazz));
    }

    public static KafkaProducer<String, String> getProducer() {
        return new KafkaProducer<>(getProducerProperties());
    }

    private static String getRandomId() {
        return String.valueOf(new Random().nextInt(1000));
    }
}
