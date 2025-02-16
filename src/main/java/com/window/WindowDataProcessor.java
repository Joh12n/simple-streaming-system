package com.window;

import com.api.*;
import com.config.StreamConfig;
import com.operator.MapOperator;
import com.operator.ReduceOperator;
import com.operator.SinkOperator;
import com.operator.SourceOperator;
import com.test.WordCount;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class WindowDataProcessor {

    public static void processWindowData(StreamConfig config, SourceOperator<String> sourceOperator, SinkOperator<WordCount> sinkOperator, List<String> windowData) {
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