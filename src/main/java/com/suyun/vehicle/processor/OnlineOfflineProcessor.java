package com.suyun.vehicle.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.suyun.common.kafka.JsonSerializer;
import com.suyun.common.lang.Tuple;
import com.suyun.common.lang.Tuple2;
import com.suyun.vehicle.Topics;
import com.suyun.vehicle.VehiclePartsCodes;
import com.suyun.vehicle.api.dto.AccDTO;
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

    public final static String ONLINE_OFFLINE_STORE = "vehicle_online_offline_store";
    private final static Logger LOGGER = LoggerFactory.getLogger(OnlineOfflineProcessor.class);
    private final static int ONLINE = 1;
    private final static int OFFLINE = 2;


    public void process(KStream<String, byte[]> stream, KStreamBuilder builder) {

        StateStoreSupplier store = Stores.create(ONLINE_OFFLINE_STORE)
                .withKeys(Serdes.String())
                .withValues(Serdes.ByteArray())
                .persistent()
                .build();

        builder.addStateStore(store);

        stream.filter((key, value) -> {
            List<VehicleData> vehicleDatas = deserialize(value);
            // 国标: 根据"车辆状态"来获取 / 部标: 根据"ACC状态"来取
            Optional<VehicleData> vehStatus = getVehStatus(vehicleDatas);
            if (vehStatus.isPresent()) {
                VehicleData vehicleData = vehStatus.get();
                String metricCode = vehicleData.getMetricCode();

                if (metricCode.equals(VehiclePartsCodes.BUS_ACC_STATUS)) { //部标
                    return true;

                } else if (metricCode.equals(VehiclePartsCodes.GB_VEH_STATUS)) { //国标
                    double status = vehicleData.getValue();
                    // 只过滤出 "启动"/"熄火"　状态的数据
                    if (status == 0x01 || status == 0x02) {
                        return true;
                    }
                }
            }

            return false;
        })

        .map((key, value) -> {

            List<VehicleData> vehicleDatas = deserialize(value);
            VehicleData vehStatus = getVehStatus(vehicleDatas).get();
            int status = vehStatus.getValue().intValue();
            int out;
            if (vehStatus.getMetricCode().equals(VehiclePartsCodes.GB_VEH_STATUS)) { //国标
                out = status == 0x01 ? ONLINE : OFFLINE;
            } else { //部标
                out = status == 1 ? ONLINE : OFFLINE;
            }

            AccDTO result = new AccDTO(vehStatus.getVehicleId(), out, vehStatus.getTimestamp());
            return new KeyValue<>(vehStatus.getVehicleId(), result);
        })

        .transform(() -> new Transformer<String, AccDTO, KeyValue<String, byte[]>>() {
            private ProcessorContext context;
            private KeyValueStore<String, byte[]> state;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.state = (KeyValueStore<String, byte[]>) context.getStateStore(ONLINE_OFFLINE_STORE);
                context.schedule(1000);  // call #punctuate() each 1000ms
            }

            @Override
            public KeyValue<String, byte[]> transform(String key, AccDTO value) {
                AccDTO currState = null;
                try {
                    currState = deserilizeStoreValue(this.state.get(key));
                } catch (Exception e) {
                    LOGGER.info("deserialize exception" + e);
                }

                if (LOGGER.isDebugEnabled() && currState != null) {
                    LOGGER.debug("Vehicle: " + key + " status is "+currState);
                }

                this.state.put(key, JsonSerializer.serialize(value));

                if (currState!= null && value.getAccStatus() == currState.getAccStatus()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Vehicle status not changed");
                    }
                    return null;
                }
                else {
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
        return findByCodes(vehicleData, VehiclePartsCodes.GB_VEH_STATUS, VehiclePartsCodes.BUS_ACC_STATUS);
    }

    public static AccDTO deserilizeStoreValue(byte[] bytes) {
        return JsonSerializer.deserialize(bytes, AccDTO.class);
    }

}
