package com.suyun.vehicle.app;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Query from kafka stream
 * Created by IT on 2017/4/11.
 */
@Component
@Configuration
@ComponentScan({"com.suyun.vehicle", "com.suyun.common"})
@PropertySource("classpath:app.properties")
public class QueryDemo {

    @Autowired
    private StreamsConfig streamsConfig;

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(StreamProcessorApp.class);
        QueryDemo app = context.getBean(QueryDemo.class);
        app.start();
    }

    private void start() {
        KStreamBuilder builder = new KStreamBuilder();
        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        //streams.start();

        System.out.println("--------------------------------");
        ReadOnlyKeyValueStore<String, Integer> onlineOfflineStore = streams.store("vehicle_online_offline_store", QueryableStoreTypes.<String, Integer>keyValueStore());
        KeyValueIterator<String, Integer> all = onlineOfflineStore.all();
        while (all.hasNext()) {
            KeyValue<String, Integer> item = all.next();
            System.out.println(item.key + ": "+item.value);
        }
    }
}
