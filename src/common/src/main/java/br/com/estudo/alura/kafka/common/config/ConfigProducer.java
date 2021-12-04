package br.com.estudo.alura.kafka.common.config;

import br.com.estudo.alura.kafka.common.infra.GsonKafkaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ConfigProducer {

    public static Properties getProducerProperties() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.HOST);

        // tell kafka that the message will be a string
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public static KafkaProducer<String, String> getProducer() {
        return new KafkaProducer<>(getProducerProperties());
    }
}
