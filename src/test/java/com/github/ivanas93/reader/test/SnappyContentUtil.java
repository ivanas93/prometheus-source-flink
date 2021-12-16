package com.github.ivanas93.reader.test;

import lombok.SneakyThrows;
import org.xerial.snappy.Snappy;
import prometheus.Remote;
import prometheus.Types;

import java.util.List;
import java.util.Map;

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

            Types.Label.Builder labelBuilder = Types.Label.newBuilder();
            Types.Sample.Builder sampleBuilder = Types.Sample.newBuilder();

            labels.forEach((k, v) -> labelBuilder.setName(k).setValue(v));
            Types.Label l = labelBuilder.build();

            sampleBuilder.setValue(value).setTimestamp(timestampInSecond);
            Types.Sample s = sampleBuilder.build();

            Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();
            timeSeriesBuilder.addAllLabels(List.of(l));
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

