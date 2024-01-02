package com.amazonaws.samples.stream.temperature.event;

import com.amazonaws.samples.stream.temperature.enrichment.async.AsyncEnrichmentFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.samples.stream.temperature.ProcessTemperatureStream.DEFAULT_API_URL;
import static com.amazonaws.samples.stream.temperature.enrichment.async.AsyncProcessTemperatureStreamStrategy.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnrichedTemperatureIntegrationTest
{
    private static final int PARALLELISM = 2;

    /** This isn't necessary, but speeds up the tests. */
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());


    @Test
    public void testAsyncEnrichment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        var temperatureDataStream = env.fromElements(
                new Temperature("sensor_1", System.currentTimeMillis(), 270L, "OK"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .keyBy(Temperature::getSensorId);

        SingleOutputStreamOperator<EnrichedTemperature> asyncEnrichedTemperatureSingleOutputStream =
                AsyncDataStream
                        .unorderedWait(
                                temperatureDataStream,
                                new AsyncEnrichmentFunction(DEFAULT_API_URL),
                                ASYNC_OPERATOR_TIMEOUT,
                                TimeUnit.MILLISECONDS,
                                ASYNC_OPERATOR_CAPACITY)
                        .name("Async enrichment")
                        .uid("Async enrichment");

        asyncEnrichedTemperatureSingleOutputStream.addSink(new CollectSink());

        env.execute();

        // verify your results
        assertTrue(CollectSink.values.size() > 0);

    }

    private static class CollectSink implements SinkFunction<EnrichedTemperature> {

        // must be static
        public static final List<EnrichedTemperature> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(EnrichedTemperature value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
