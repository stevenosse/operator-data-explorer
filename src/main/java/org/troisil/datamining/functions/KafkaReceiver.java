package org.troisil.datamining.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Map;
import java.util.function.Supplier;

@RequiredArgsConstructor
@Slf4j
public class KafkaReceiver implements Supplier<JavaDStream<String>> {
    private final Config config = ConfigFactory.load("application.conf");
    private final List<String> topics = config.getStringList("app.kafka.topics");
    private final String brokers = config.getString("app.kafka.brokers");

    private final JavaStreamingContext javaStreamingContext;

    private final Map<String, Object> kafkaParams = new HashMap<String, Object>(){{
        put("bootstrap.servers", brokers);
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", "1");
        put("auto.offset.reset", "earliest");
        put("enable.auto.commit", false);
    }};

    @Override
    public JavaDStream<String> get() {
        JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        return kafkaDStream.map(ConsumerRecord::value);
    }
}