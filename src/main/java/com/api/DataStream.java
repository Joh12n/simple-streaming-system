package com.api;

import java.util.List;

public class DataStream<T> {
    private List<T> data;

    public DataStream(List<T> data) {
        this.data = data;
    }

    public List<T> getData() {
        return data;
    }
}