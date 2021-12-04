package br.com.estudo.alura.kafka.common.infra;

import org.apache.kafka.common.serialization.Deserializer;

public class GsonKafkaDeserializer<T> implements Deserializer {
    @Override
    public Object deserialize(String topic, byte[] data) {
        throw new UnsupportedOperationException("implement it");
    }
}
