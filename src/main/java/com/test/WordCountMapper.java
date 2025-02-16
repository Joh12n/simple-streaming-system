package com.test;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class WordCountMapper {

    public static Function<String, List<WordCount>> getMapper() {
        return text -> {
            List<WordCount> wordCounts = new ArrayList<>();
            String[] words = text.split(" ");
            for (String word : words) {
                wordCounts.add(new WordCount(word, 1));
            }
            return wordCounts;
        };
    }

    public static Function<String, WordCount> getFlatMapper() {
        Function<String, List<WordCount>> mapper = getMapper();
        return text -> {
            List<WordCount> wordCounts = mapper.apply(text);
            return wordCounts.isEmpty() ? null : wordCounts.get(0);
        };
    }
}