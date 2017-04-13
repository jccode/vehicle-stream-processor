package com.suyun.vehicle.service.impl;

import com.suyun.common.lang.Tuple2;
import com.suyun.vehicle.api.dto.AccDTO;
import com.suyun.vehicle.api.services.VehicleStreamService;
import com.suyun.vehicle.processor.OnlineOfflineProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Vehicle stream service impl
 *
 * Created by IT on 2017/4/11.
 */
@Service("vehicleStreamService")
public class VehicleStreamServiceImpl implements VehicleStreamService {

    @Autowired
    private KafkaStreams streams;

    private Logger LOGGER = LoggerFactory.getLogger(VehicleStreamServiceImpl.class);

    public VehicleStreamServiceImpl() {
    }

    @Override
    public List<AccDTO> findVehicleAccStatus() {
        List<AccDTO> result = new ArrayList<>();
        ReadOnlyKeyValueStore<String, byte[]> onlineOfflineStore = streams.store(OnlineOfflineProcessor.ONLINE_OFFLINE_STORE, QueryableStoreTypes.<String, byte[]>keyValueStore());
        KeyValueIterator<String, byte[]> iter = onlineOfflineStore.all();
        while (iter.hasNext()) {
            KeyValue<String, byte[]> item = iter.next();
            AccDTO value = OnlineOfflineProcessor.deserilizeStoreValue(item.value);
            result.add(value);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Vehicle acc status: ");
            LOGGER.debug(result.toString());
        }
        return result;
    }
}
