package com;
import com.api.*;
import com.config.StreamConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class Main {
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
                processWindowData(config, sourceOperator, sinkOperator);
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

    private static void processWindowData(StreamConfig config, SourceOperator<String> sourceOperator, SinkOperator<WordCount> sinkOperator) {
        // 复制当前窗口数据并清空窗口
        List<String> currentWindowData = new ArrayList<>(windowData);
        windowData.clear();

        if (currentWindowData.isEmpty()) {
            System.out.println("No data in the current 5-minute window.");
            return;
        }

        DataStream<String> sourceData = new DataStream<>(currentWindowData);

        // 自定义 map 操作
        Function<String, List<WordCount>> mapper = text -> {
            List<WordCount> wordCounts = new ArrayList<>();
            String[] words = text.split(" ");
            for (String word : words) {
                wordCounts.add(new WordCount(word, 1));
            }
            return wordCounts;
        };

        // 为了适配 MapOperator，需要将 Function<String, List<WordCount>> 转换为 Function<String, WordCount>
        Function<String, WordCount> flatMapper = text -> {
            List<WordCount> wordCounts = mapper.apply(text);
            return wordCounts.isEmpty() ? null : wordCounts.get(0);
        };

        MapOperator<String, WordCount> mapOperator = new MapOperator<>(flatMapper);
        DataStream<WordCount> mappedData = mapOperator.apply(sourceData);

        // 自定义 reduce 操作
        Function<WordCount, String> keySelector = WordCount::getWord;
        BinaryOperator<WordCount> reducer = (wc1, wc2) -> {
            return new WordCount(wc1.getWord(), wc1.getCount() + wc2.getCount());
        };

        ReduceOperator<WordCount, String> reduceOperator = new ReduceOperator<>(keySelector, reducer);
        DataStream<WordCount> reducedData = reduceOperator.apply(mappedData);

        // 使用 SinkOperator 输出结果
        sinkOperator.apply(reducedData);
    }
}