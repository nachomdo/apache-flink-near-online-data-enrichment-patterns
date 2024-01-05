package com.amazonaws.samples.stream.temperature.enrichment.sync;

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
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

import static com.amazonaws.samples.stream.temperature.ProcessTemperatureStream.*;


public class SyncProcessTemperatureStreamStrategy implements EnrichmentStrategy {

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {
        String kafkaBrokers = parameter.get("bootstrapServers", DEFAULT_KAFKA_BROKERS);

        KafkaSource<Temperature> source = KafkaSource.<Temperature>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(parameter.get("InputStreamName", DEFAULT_INPUT_STREAM_NAME))
                .setGroupId("flink-sync-kafka-connector")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TemperatureDeserializationSchema())
                .build();

        DataStream<Temperature> temperatureDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Temperature input");

        DataStream<EnrichedTemperature> enrichedTemperatureDataStream =
                temperatureDataStream
                        .map(new SyncEnrichmentFunction(parameter.get("SensorApiUrl", DEFAULT_API_URL)))
                        .name("Sync enrichment")
                        .uid("Sync enrichment");

        enrichedTemperatureDataStream
                .sinkTo(KafkaSink.<EnrichedTemperature>builder()
                        .setBootstrapServers(kafkaBrokers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(parameter.get("OutputStreamName", DEFAULT_OUTPUT_STREAM_NAME))
                                .setValueSerializationSchema(new TemperatureSerializationSchema())
                                .build())
                        .build())
                .name("Sync output")
                .uid("Sync output");

        LOG.info("Reading events from stream");

        // execute program
        env.execute("Flink Streaming Temperature Synchronous Enrichment");
    }
}

