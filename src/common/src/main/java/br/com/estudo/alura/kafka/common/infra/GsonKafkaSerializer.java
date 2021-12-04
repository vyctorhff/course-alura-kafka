package br.com.estudo.alura.kafka.common.infra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class GsonKafkaSerializer<T> implements Serializer<T> {

    private static final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, T data) {
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
}
