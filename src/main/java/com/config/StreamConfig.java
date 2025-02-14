package com.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StreamConfig {
    private Properties properties;

    public StreamConfig(String configFilePath) {
        properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFilePath)) {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new IOException("Unable to load properties file: " + configFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}