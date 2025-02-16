package com.operator;

import com.api.DataStream;
import com.config.StreamConfig;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class SinkOperator<T> {
    private String outputFilePath;

    public SinkOperator(StreamConfig config) {
        this.outputFilePath = config.getProperty("output.file.path");
        if (this.outputFilePath == null || this.outputFilePath.isEmpty()) {
            throw new IllegalArgumentException("Output file path is not configured in stream-config.properties.");
        }
    }

    public void apply(DataStream<T> input) {
        List<T> data = input.getData();
        if (data.isEmpty()) {
            System.out.println("No data to write to the output file.");
            return;
        }

        try (FileWriter writer = new FileWriter(outputFilePath, true)) {
            writer.write("Results for window starting at: " + Instant.now() + "\n");
            for (T item : data) {
                writer.write(item.toString() + "\n");
            }
            writer.write("------------------------\n");
            System.out.println("Data has been written to " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Error writing to output file: " + e.getMessage());
        }
    }
}
