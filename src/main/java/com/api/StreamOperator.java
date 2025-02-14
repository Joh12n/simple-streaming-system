package com.api;

public interface StreamOperator<IN, OUT> {
    DataStream<OUT> apply(DataStream<IN> input);
    void setParallelism(int parallelism);
}