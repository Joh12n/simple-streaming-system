package com.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class KeybyOperator<T, K> implements StreamOperator<T, T> {
    private int parallelism;
    private Function<T, K> keySelector;

    public KeybyOperator(Function<T, K> keySelector) {
        this.keySelector = keySelector;
    }

    @Override
    public DataStream<T> apply(DataStream<T> input) {
        Map<K, List<T>> keyedData = new HashMap<>();
        List<T> inputData = input.getData();
        for (T item : inputData) {
            K key = keySelector.apply(item);
            keyedData.computeIfAbsent(key, k -> new ArrayList<>()).add(item);
        }
        List<T> outputData = new ArrayList<>();
        for (List<T> list : keyedData.values()) {
            outputData.addAll(list);
        }
        return new DataStream<>(outputData);
    }

    @Override
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}