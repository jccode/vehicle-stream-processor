package com.suyun.vehicle.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.suyun.common.kafka.JsonSerializer;
import com.suyun.vehicle.model.VehicleData;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Vehicle data processor
 *
 * Created by IT on 2017/4/7.
 */
public abstract class VehicleDataProcessor {

    private TypeReference<List<VehicleData>> dataType = new TypeReference<List<VehicleData>>() {
    };

    protected List<VehicleData> deserialize(byte[] data) {
        return JsonSerializer.deserialize(data, dataType);
    }

    protected Optional<VehicleData> findByCode(List<VehicleData> list, String code) {
        return list.stream().filter(d -> d.getMetricCode().equals(code)).findFirst();
    }

    protected Optional<VehicleData> findByCodes(List<VehicleData> list, String ...codes) {
        return list.stream().filter(d -> Arrays.stream(codes).anyMatch(code -> code.equals(d.getMetricCode()))).findFirst();
    }

    Stream<VehicleData> filterByCodes(List<VehicleData> list, String ...codes) {
        return list.stream().filter(d -> Arrays.stream(codes).anyMatch(code -> code.equals(d.getMetricCode())));
    }

    public abstract void process(KStream<String, byte[]> stream, KStreamBuilder builder);
}
