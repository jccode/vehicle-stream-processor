package com.suyun.vehicle.processor;

import com.suyun.common.kafka.JsonSerializer;
import com.suyun.vehicle.Topics;
import com.suyun.vehicle.VehiclePartsCodes;
import com.suyun.vehicle.model.VehicleData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Online / Offline Processor
 * 上线/下线 处理器
 *
 * Created by IT on 2017/4/7.
 */
@Component
public class OnlineOfflineProcessor extends VehicleDataProcessor {

    private final static String ONLINE_OFFLINE_STORE = "vehicle_online_offline_store";
    private final static Logger LOGGER = LoggerFactory.getLogger(OnlineOfflineProcessor.class);

    public void process(KStream<String, byte[]> stream, KStreamBuilder builder) {

        StateStoreSupplier store = Stores.create(ONLINE_OFFLINE_STORE)
                .withKeys(Serdes.String())
                .withValues(Serdes.Integer())
                .persistent()
                .build();

        builder.addStateStore(store);

        stream.filter((key, value) -> {
            List<VehicleData> vehicleData = deserialize(value);
            // 国标: 根据"车辆状态"来获取
            Optional<VehicleData> vehStatus = getVehStatus(vehicleData);
            if (vehStatus.isPresent()) {
                double status = vehStatus.get().getValue();
                // 只过滤出 "启动"/"熄火"　状态的数据
                if (status == 0x01 || status == 0x02) {
                    return true;
                }
            }

            // TODO: 部标: 要根据什么来获取?
            return false;
        })

        .map((key, value) -> {

            List<VehicleData> vehicleDatas = deserialize(value);
            VehicleData vehStatus = getVehStatus(vehicleDatas).get();
            return new KeyValue<>(vehStatus.getVehicleId(), vehStatus.getValue().intValue());
        })

        .transform(() -> new Transformer<String, Integer, KeyValue<String, byte[]>>() {
            private ProcessorContext context;
            private KeyValueStore<String, Integer> state;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.state = (KeyValueStore<String, Integer>) context.getStateStore(ONLINE_OFFLINE_STORE);
                context.schedule(1000);  // call #punctuate() each 1000ms
            }

            @Override
            public KeyValue<String, byte[]> transform(String key, Integer value) {
                Integer currState = this.state.get(key);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Vehicle: " + key + " status is "+currState);
                }

                this.state.put(key, value);

                if (value.equals(currState)) {

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Vehicle status not changed");
                    }

                    return null;
                }
                else {
//                    return new KeyValue<>(key, value);
                    return new KeyValue<>(key, JsonSerializer.serialize(value));
                }
            }

            @Override
            public KeyValue<String, byte[]> punctuate(long timestamp) {
                return null;
            }


            @Override
            public void close() {
                this.state.close();
            }

        }, ONLINE_OFFLINE_STORE)

        .to(Topics.ONLINE_OFFLINE);

    }

    private Optional<VehicleData> getVehStatus(List<VehicleData> vehicleData) {
        return findByCode(vehicleData, VehiclePartsCodes.GB_VEH_STATUS);
    }
}