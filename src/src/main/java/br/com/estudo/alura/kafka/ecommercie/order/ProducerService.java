package br.com.estudo.alura.kafka.ecommercie.order;

import br.com.estudo.alura.kafka.ecommercie.config.Config;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var key = "123";
        var content = "id:1123, product:15, price:200";

        var producer = Config.getProducer();
        Callback callback = getCallback();

        var recordOrder = new ProducerRecord<>(Config.TOPIC_NEW_ORDER, key, content);
        var recordEmail = new ProducerRecord<>(Config.TOPIC_NEW_ORDER_EMAIL, key, "content@email.com");

        producer.send(recordOrder, callback).get();
        producer.send(recordEmail, callback).get();
    }

    private static Callback getCallback() {
        Callback callback = (data, err) -> {
            if (err != null) {
                LOGGER.error("", err);
                return;
            }

            System.out.printf("success - topic: %s, offset: %s, partition: %s, time: %s%n",
                    data.topic(),
                    data.offset(),
                    data.partition(),
                    data.timestamp()
            );
        };
        return callback;
    }
}
