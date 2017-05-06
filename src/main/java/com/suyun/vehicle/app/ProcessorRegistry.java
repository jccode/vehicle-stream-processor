package com.suyun.vehicle.app;

import com.google.common.collect.Lists;
import com.suyun.vehicle.processor.OnlineOfflineProcessor;
import com.suyun.vehicle.processor.StatisticsProcessor;
import com.suyun.vehicle.processor.VehicleDataProcessor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Processor registry
 *
 * Created by IT on 2017/5/4.
 */
@Component
public class ProcessorRegistry {

    public List<VehicleDataProcessor> getProcessors() {
        return Lists.newArrayList(
                new OnlineOfflineProcessor(),
                new StatisticsProcessor()
        );
    }
}
