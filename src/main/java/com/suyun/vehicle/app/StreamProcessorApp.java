package com.suyun.vehicle.app;

import com.suyun.vehicle.processor.OnlineOfflineProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Stream processor Application
 *
 * Created by IT on 2017/4/5.
 */
@Component
@Configuration
@ComponentScan({"com.suyun.vehicle", "com.suyun.common"})
@PropertySource("classpath:app.properties")
public class StreamProcessorApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamProcessorApp.class);

    private String TOPIC_VEHICLE_DATA = "vehicle_data";

    @Autowired
    private StreamsConfig streamsConfig;

    @Autowired
    private OnlineOfflineProcessor processor;

    public static void main(String[] args) throws IOException {
        ApplicationContext context = new AnnotationConfigApplicationContext(StreamProcessorApp.class);
        StreamProcessorApp app = context.getBean(StreamProcessorApp.class);
        app.start();
    }

    private void start() throws IOException {
        LOGGER.info("-------------------------------------------");
        LOGGER.info("    Stream processor application started.  ");
        LOGGER.info("-------------------------------------------");
        startStreamProcessing();
    }




    private void startStreamProcessing() {
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, byte[]> stream = builder.stream(TOPIC_VEHICLE_DATA);
        processor.process(stream, builder);

        /*
        stream.filter((key, value) -> deserialize((byte[]) value).length() > 5).mapValues(value -> {
            String deserialize = deserialize((byte[]) value);
            System.out.println("R: " + deserialize);
            //return value;
            return ("R" + deserialize + deserialize.length()).getBytes();
        }).through(topicOut);


        stream.mapValues(value -> {
            String deserialize = deserialize((byte[]) value);
            System.out.println("R: " + deserialize);
            return (deserialize.length()+"").getBytes();
        }).through("test_out2");
        */


        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
