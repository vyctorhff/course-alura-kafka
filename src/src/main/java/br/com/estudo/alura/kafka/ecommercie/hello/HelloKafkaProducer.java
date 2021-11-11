package br.com.estudo.alura.kafka.ecommercie.hello;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class HelloKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloKafkaProducer.class);

    public static final String HOST = "localhost:9092";
    public static final String TOPIC = "ECOMMERCE-NEW-ORDER";

    public static Properties getProperties() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);

        // tell kafka that the message will be a string
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var content = "id:1123, product:15, price:200";

        var producer = new KafkaProducer<String, String>(getProperties());
        var record = new ProducerRecord<>(TOPIC, content, content);

        /*
        if the topic do not exists, kafka will return a error but also will create it.
        Next time will work.
         */

        // this call wil wait the message being sent
        // Have to passing a listener(observable).
        producer.send(record, (data, err) -> {
            if (err != null) {
                LOGGER.error("", err);
                return;
            }

            System.out.println(String.format("success - topic: %s, offset: %s, partition: %s, time: %s",
                data.topic(),
                data.offset(),
                data.partition(),
                data.timestamp()
            ));
        }).get();

        // offset increase by 1 every time we send a message
    }
}
