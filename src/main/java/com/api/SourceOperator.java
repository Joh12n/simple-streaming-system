package com.api;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SourceOperator<T> {
    private static final Logger logger = LoggerFactory.getLogger(SourceOperator.class);
    private String topic;
    private Properties kafkaProps;

    public SourceOperator(String topic, Properties kafkaProps) {
        this.topic = topic;
        this.kafkaProps = kafkaProps;
    }

    public DataStream<T> apply(DataStream<Void> input) {
        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Arrays.asList(topic));

        List<T> data = new ArrayList<>();
        ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
        logger.info("Polled {} records from Kafka topic: {}", records.count(), topic);

        for (ConsumerRecord<String, T> record : records) {
            data.add(record.value());
        }

        return new DataStream<>(data);
    }
}