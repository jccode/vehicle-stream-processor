package com.suyun.vehicle.app;

import com.suyun.vehicle.Topics;
import com.suyun.vehicle.processor.OnlineOfflineProcessor;
import com.suyun.vehicle.processor.StatisticsProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
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
@ImportResource("classpath:provider.xml")
public class StreamProcessorApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamProcessorApp.class);

    @Autowired
    private StreamsConfig streamsConfig;

//    @Autowired
//    private OnlineOfflineProcessor processor;

//    @Autowired
//    private StatisticsProcessor statisticsProcessor;

    @Autowired
    private ProcessorRegistry registry;

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

    @Bean
    public KafkaStreams createStream() {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> stream = builder.stream(Topics.VEHICLE_DATA);
        registry.getProcessors().forEach(processor -> {
            processor.process(stream, builder);
        });
        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        return streams;
    }


    private void startStreamProcessing() {

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

        KafkaStreams streams = createStream();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
