package com.amazonaws.samples.stream.temperature.enrichment.async;

import com.amazonaws.samples.stream.temperature.enrichment.EnrichmentStrategy;
import com.amazonaws.samples.stream.temperature.event.EnrichedTemperature;
import com.amazonaws.samples.stream.temperature.event.Temperature;
import com.amazonaws.samples.stream.temperature.serialize.TemperatureDeserializationSchema;
import com.amazonaws.samples.stream.temperature.serialize.TemperatureSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

import static com.amazonaws.samples.stream.temperature.ProcessTemperatureStream.*;


public class AsyncProcessTemperatureStreamStrategy implements EnrichmentStrategy {

    public static final int ASYNC_OPERATOR_CAPACITY = 1_000;
    public static final int ASYNC_OPERATOR_TIMEOUT = 10_000;

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {
        String kafkaBrokers = parameter.get("bootstrapServers", DEFAULT_KAFKA_BROKERS);

        KafkaSource<Temperature> source = KafkaSource.<Temperature>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(parameter.get("InputStreamName", DEFAULT_INPUT_STREAM_NAME))
                .setGroupId("flink-async-kafka-connector")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TemperatureDeserializationSchema())
                .build();

        var temperatureDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Temperature input");

        SingleOutputStreamOperator<EnrichedTemperature> asyncEnrichedTemperatureSingleOutputStream =
                AsyncDataStream
                        .unorderedWait(
                                temperatureDataStream,
                                new AsyncEnrichmentFunction(parameter.get("SensorApiUrl", DEFAULT_API_URL)),
                                ASYNC_OPERATOR_TIMEOUT,
                                TimeUnit.MILLISECONDS,
                                ASYNC_OPERATOR_CAPACITY)
                        .name("Async enrichment")
                        .uid("Async enrichment");

        asyncEnrichedTemperatureSingleOutputStream
                .sinkTo(
                        KafkaSink.<EnrichedTemperature>builder()
                                .setBootstrapServers(kafkaBrokers)
                                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                        .setTopic(parameter.get("OutputStreamName", DEFAULT_OUTPUT_STREAM_NAME))
                                        .setValueSerializationSchema(new TemperatureSerializationSchema())
                                        .build())
                               .build())
                .name("Async output")
                .uid("Async output");

        LOG.info("Reading events from stream");

        // execute program
        env.execute("Flink Streaming Temperature Asynchronous Enrichment");
    }
}

