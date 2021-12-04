package br.com.estudo.alura.kafka.common.config;

import br.com.estudo.alura.kafka.common.infra.GsonKafkaDeserializer;
import br.com.estudo.alura.kafka.common.infra.GsonKafkaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConfigConsumer {

    public static KafkaConsumer<String, String> getConsumer(Class<?> clazz) {
        return new KafkaConsumer<>(getConsumerProperties(clazz));
    }

    public static KafkaConsumer<String, String> getConsumerWithJson(Class<?> clazz, boolean json) {
        return new KafkaConsumer<>(getConsumerJsonProperties(clazz));
    }

    public static Properties getConsumerProperties(Class<?> clazz) {
        var props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clazz.getSimpleName());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, Config.getRandomId());
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        return props;
    }

    public static Properties getConsumerJsonProperties(Class<?> clazz) {
        var props = getConsumerProperties(clazz);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GsonKafkaSerializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonKafkaDeserializer.class.getName());
        return props;
    }
}
