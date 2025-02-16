package com.test;

import java.util.function.BinaryOperator;
import java.util.function.Function;

public class WordCountReducer {

    public static Function<WordCount, String> getKeySelector() {
        return WordCount::getWord;
    }

    public static BinaryOperator<WordCount> getReducer() {
        return (wc1, wc2) -> {
            return new WordCount(wc1.getWord(), wc1.getCount() + wc2.getCount());
        };
    }
}