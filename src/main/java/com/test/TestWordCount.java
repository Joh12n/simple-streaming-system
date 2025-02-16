package com.test;

import com.api.*;
import com.config.StreamConfig;
import com.operator.SinkOperator;
import com.operator.SourceOperator;
import com.window.WindowDataProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class TestWordCount {
    private static final long WINDOW_SIZE = 5 * 60 * 1000; // 5 minutes in milliseconds
    private static List<String> windowData = new ArrayList<>();

    public static void main(String[] args) {
        // 读取配置文件
        StreamConfig config = new StreamConfig("stream-config.properties");

        // Kafka 配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("group.id", config.getProperty("kafka.group.id"));
        kafkaProps.put("auto.offset.reset", "earliest");

        // 从 Kafka 主题读取数据
        String topic = config.getProperty("kafka.topic");
        SourceOperator<String> sourceOperator = new SourceOperator<>(topic, kafkaProps);

        // 创建 SinkOperator 实例
        SinkOperator<WordCount> sinkOperator = new SinkOperator<>(config);

        // 定时任务，每 5 分钟处理一次窗口数据
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                WindowDataProcessor.processWindowData(config, sourceOperator, sinkOperator, windowData);
            }
        }, WINDOW_SIZE, WINDOW_SIZE);

        // 持续从 Kafka 拉取数据并添加到窗口
        while (true) {
            DataStream<String> sourceData = sourceOperator.apply(null);
            List<String> newData = sourceData.getData();
            windowData.addAll(newData);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}