package com.window;

import com.api.DataStream;
import com.api.WordCount;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TumblingProcessingTimeWindow {
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private LocalDateTime currentWindowStart;
    private Map<String, Integer> wordCountMap;

    public TumblingProcessingTimeWindow() {
        currentWindowStart = LocalDateTime.now().truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        wordCountMap = new HashMap<>();
    }

    public List<String> apply(DataStream<WordCount> input) {
        List<WordCount> data = input.getData();
        List<String> results = new ArrayList<>();

        for (WordCount wc : data) {
            LocalDateTime currentTime = LocalDateTime.now();
            if (currentTime.isAfter(currentWindowStart.plus(WINDOW_SIZE))) {
                // 窗口触发，输出结果
                String windowEndTime = currentWindowStart.plus(WINDOW_SIZE).format(DateTimeFormatter.ofPattern("yyyy/MM/dd'T'HH:mm:ss"));
                for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                    results.add(windowEndTime + "," + entry.getKey() + "," + entry.getValue());
                }
                // 重置窗口和计数
                currentWindowStart = currentTime.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
                wordCountMap.clear();
            }
            // 统计词频，忽略大小写
            String word = wc.getWord().toLowerCase();
            wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + wc.getCount());
        }

        return results;
    }
}