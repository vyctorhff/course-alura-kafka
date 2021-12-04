package br.com.estudo.alura.kafka.hello;

import br.com.estudo.alura.kafka.common.config.Config;
import br.com.estudo.alura.kafka.common.config.ConfigProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class HelloKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloKafkaProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var content = "id:1123, product:15, price:200";

        var producer = ConfigProducer.getProducer();
        var record = new ProducerRecord<>(Config.TOPIC_HELLO, content, content);

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
