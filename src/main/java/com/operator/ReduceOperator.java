package com.operator;

import com.api.DataStream;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class ReduceOperator<T, K> {
    private Function<T, K> keySelector;
    private BinaryOperator<T> reducer;

    public ReduceOperator(Function<T, K> keySelector, BinaryOperator<T> reducer) {
        this.keySelector = keySelector;
        this.reducer = reducer;
    }

    public DataStream<T> apply(DataStream<T> input) {
        Map<K, T> reducedData = new HashMap<>();
        List<T> inputData = input.getData();
        for (T item : inputData) {
            K key = keySelector.apply(item);
            reducedData.merge(key, item, reducer);
        }
        return new DataStream<>(new ArrayList<>(reducedData.values()));
    }
}