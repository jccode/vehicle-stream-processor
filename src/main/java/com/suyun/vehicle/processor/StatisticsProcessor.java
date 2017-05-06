package com.suyun.vehicle.processor;

import com.suyun.common.kafka.JsonSerializer;
import com.suyun.vehicle.VehiclePartsCodes;
import com.suyun.vehicle.model.VehicleData;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 统计处理器
 *
 * Created by IT on 2017/5/4.
 */
public class StatisticsProcessor extends VehicleDataProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(StatisticsProcessor.class);
    private final static String TOPIC = "stat_minute";
    private final static long WINDOW_SIZE = 1 * 60 * 1000l; // 1 min
    private final static String STAT_STORE = "vehicle_stat_store";

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer,stringDeserializer);
    WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
    WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
    Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);




    @SuppressWarnings("unchecked")
    @Override
    public void process(KStream<String, byte[]> stream, KStreamBuilder builder) {

        // 统计多个值.
        stream
                .flatMap((key, value) -> {
                    List<VehicleData> vehicleDatas = deserialize(value);
                    Stream<KeyValue<String, Double>> keyValueStream = vehicleDatas.stream()
                            .filter(v ->
                                    Arrays.stream(interestCodes()).anyMatch(code ->
                                            code.equals(v.getMetricCode())))
                            .map(v -> new KeyValue(v.getVehicleId() + ":" + v.getMetricCode(), v.getValue()));
                    return keyValueStream.collect(Collectors.toList());
                })

                .groupByKey(stringSerde, Serdes.Double())

                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> {
                            LOGGER.info("[statics processor] agg: " + aggregate);
                            return (value + aggregate);
                        },
                        TimeWindows.of(WINDOW_SIZE), // tumbling windows
                        Serdes.Double(),
                        STAT_STORE
                )

                .mapValues(JsonSerializer::serialize)

                .to(windowedSerde, Serdes.ByteArray(), TOPIC);


//        DEBUG
//                .map((key, value) -> {
//                    LOGGER.info("[StatisticsProcessor]" + key + ": " + value);
//                    return new KeyValue<>(key, JsonSerializer.serialize(value));
//                })
//                .to(stringSerde, Serdes.ByteArray(), TOPIC);


        /*
        // 只统计单个值.
        stream
                .filter((key, value) -> {
                    LOGGER.info("[StatisticsProcessor] ");
                    List<VehicleData> vehicleDatas = deserialize(value);
                    Optional<VehicleData> socData = getSocData(vehicleDatas);
                    return socData.isPresent();
                })

                .map((key, value) -> {
                    List<VehicleData> vehicleDatas = deserialize(value);
                    VehicleData socData = getSocData(vehicleDatas).get();
                    LOGGER.info("[StatisticsProcessor] receive: soc:" + socData.getValue());
                    return new KeyValue<>(socData.getVehicleId(), socData.getValue());
                })

                .groupByKey(stringSerde, Serdes.Double())

                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> {
                            LOGGER.info("[statics processor] agg: " + aggregate);
                            return (value + aggregate);
                        },
                        TimeWindows.of(WINDOW_SIZE), // tumbling windows
                        Serdes.Double(),
                        STAT_STORE
                )

                .mapValues(value -> {
                    LOGGER.info("[StatisticsProcessor] deserilize " + value);
                    return JsonSerializer.serialize(value);
                })

                .to(windowedSerde, Serdes.ByteArray(), TOPIC);
                */

    }

    private Optional<VehicleData> getSocData(List<VehicleData> vehicleDataList) {
        return findByCodes(vehicleDataList, VehiclePartsCodes.GB_VEH_SOC, VehiclePartsCodes.BMS_SOC);
    }

    private String[] interestCodes() {
        return new String[] {
                VehiclePartsCodes.BMS_SOC,
                VehiclePartsCodes.GB_VEH_SOC,
                VehiclePartsCodes.BUS_TOTAL_MILEAGE,
                VehiclePartsCodes.GB_VEH_TOTAL_MILEAGE
        };
    }
}
