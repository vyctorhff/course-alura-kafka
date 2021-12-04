package br.com.estudo.alura.kafka.common.config;

import java.util.Random;

public class Config {

    public static final String HOST = "localhost:9092";

    public static final String TOPIC_HELLO = "ECOMMERCE-HELLO";
    public static final String TOPIC_NEW_ORDER = "ECOMMERCE-NEW-ORDER";
    public static final String TOPIC_NEW_ORDER_EMAIL = "ECOMMERCE-NEW-ORDER-EMAIL";

    public static String getRandomId() {
        return String.valueOf(new Random().nextInt(1000));
    }
}
