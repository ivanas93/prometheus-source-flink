package com.github.ivanas93.reader.test;

import lombok.SneakyThrows;
import org.xerial.snappy.Snappy;
import prometheus.Remote;
import prometheus.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SnappyContentUtil {

    @SneakyThrows
    public static SnappyTimeSeries snappyTimeSeriesBuilder() {
        return new SnappyTimeSeries();
    }

    public static class SnappyTimeSeries {

        private final Remote.WriteRequest.Builder writeRequestBuilder;

        SnappyTimeSeries() {
            writeRequestBuilder = Remote.WriteRequest.newBuilder();
        }

        public SnappyTimeSeries add(final String metricName, final Map<String, String> labels,
                                    final float value, final long timestampInSecond) {


            Types.Sample.Builder sampleBuilder = Types.Sample.newBuilder();

            List<Types.Label> timeSeriesLabels = new ArrayList<>();
            timeSeriesLabels.add(Types.Label.newBuilder().setName("__name__").setValue(metricName).build());

            List<Types.Label> metricLabels = labels.entrySet()
                    .stream()
                    .map(e -> Types.Label.newBuilder().setName(e.getKey()).setValue(e.getValue()).build())
                    .collect(Collectors.toList());
            timeSeriesLabels.addAll(metricLabels);

            sampleBuilder.setValue(value).setTimestamp(timestampInSecond);
            Types.Sample s = sampleBuilder.build();

            Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();
            timeSeriesBuilder.addAllLabels(timeSeriesLabels);
            timeSeriesBuilder.addAllSamples(List.of(s));
            Types.TimeSeries timeSeries = timeSeriesBuilder.build();
            writeRequestBuilder.addAllTimeseries(List.of(timeSeries));
            return this;
        }

        @SneakyThrows
        public byte[] toSnappy() {
            Remote.WriteRequest message = writeRequestBuilder.build();
            return Snappy.compress(message.toByteArray());
        }
    }
}

