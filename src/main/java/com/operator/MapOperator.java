package com.operator;

import com.api.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MapOperator<IN, OUT> {
    private Function<IN, OUT> mapper;

    public MapOperator(Function<IN, OUT> mapper) {
        this.mapper = mapper;
    }

    public DataStream<OUT> apply(DataStream<IN> input) {
        List<IN> inputData = input.getData();
        List<OUT> outputData = new ArrayList<>();
        for (IN item : inputData) {
            outputData.add(mapper.apply(item));
        }
        return new DataStream<>(outputData);
    }
}
