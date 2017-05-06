package com.suyun.vehicle.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Word count demo
 *
 * Created by IT on 2017/4/8.
 */
public class WordCountDemo {

    public static void main(String[] args) {
        new WordCountDemo().wordCountExample();
    }

    private StreamsConfig streamConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.200:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new StreamsConfig(props);
    }

    private void wordCountExample() {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<byte[]> byteSerde = Serdes.ByteArray();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "test");

        KTable<String, Long> counts = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> {
                    System.out.println("key:"+key+"; value:"+value);
                    return new KeyValue<>(value, value);
                })
                .map((key, value) -> {
                    System.out.println("map2: key:"+key+"; value:"+value);
                    return new KeyValue<>(key, value);
                })
                .groupByKey()
                .count("Counts");

        // counts.print();

        counts.mapValues(value -> value+"")
                .to(stringSerde, stringSerde, "test_out");

        KafkaStreams streams = new KafkaStreams(builder, streamConfig());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}