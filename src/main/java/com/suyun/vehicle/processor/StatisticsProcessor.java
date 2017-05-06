package com.suyun.vehicle.processor;

import com.suyun.common.kafka.JsonSerializer;
import com.suyun.vehicle.VehiclePartsCodes;
import com.suyun.vehicle.model.VehicleData;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

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



    @Override
    public void process(KStream<String, byte[]> stream, KStreamBuilder builder) {

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

    }

    public Optional<VehicleData> getSocData(List<VehicleData> vehicleDataList) {
        return findByCodes(vehicleDataList, VehiclePartsCodes.GB_VEH_SOC, VehiclePartsCodes.BMS_SOC);
    }
}
