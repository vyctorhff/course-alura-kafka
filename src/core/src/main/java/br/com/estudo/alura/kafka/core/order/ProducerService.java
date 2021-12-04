package br.com.estudo.alura.kafka.core.order;

import br.com.estudo.alura.kafka.common.config.Config;
import br.com.estudo.alura.kafka.common.config.ConfigProducer;
import br.com.estudo.alura.kafka.core.order.model.Order;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

class ProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var orderProducer = new KafkaProducer<String, Order>(ConfigProducer.getProducerProperties());
        var emailProducer = new KafkaProducer<String, String>(ConfigProducer.getProducerProperties());

        Callback callback = getCallbackConfirmation();

        for (int count = 0; count < 20; count++) {
            var order = Order.getRandom();

            var recordOrder = new ProducerRecord<>(Config.TOPIC_NEW_ORDER, order.orderId(), order);
            var recordEmail = new ProducerRecord<>(Config.TOPIC_NEW_ORDER_EMAIL, order.orderId(), "example@email.com");

            orderProducer.send(recordOrder, callback).get();
            emailProducer.send(recordEmail, callback).get();
        }
    }

    private static Callback getCallbackConfirmation() {
        Callback callback = (data, err) -> {
            if (err != null) {
                LOGGER.error("Error on producer!", err);
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
