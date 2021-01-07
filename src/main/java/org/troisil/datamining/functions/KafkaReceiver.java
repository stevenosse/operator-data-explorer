package org.troisil.datamining.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<String>> {
    @NonNull
    private final List<String> topics;

    @NonNull
    private final JavaStreamingContext javaStreamingContext;

    private final HashMap<String, Object> kafkaParams = new HashMap<String, Object>() {{
       put("bootstrap.server", "localhost:20111");
       put("key.deserializer", StringDeserializer.class);
       put("value.deserializer", StringDeserializer.class);
       put("group.id", "spark-kafka-integ");
       put("auto.offset.reset", "earlist");
    }};

    @Override
    public JavaDStream<String> get() {
        JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        return kafkaDStream.map(ConsumerRecord::value);
    }
}
