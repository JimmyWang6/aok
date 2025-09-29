package com.aok.core.config;

import lombok.Getter;

import java.util.Properties;

public class AmqpConfig {

    public static final String SERVER_HOST_CONFIG = "server.host";

    public static final String SERVER_PORT_CONFIG = "server.port";

    public static final String DEFAULT_SERVER_HOST_CONFIG = "0.0.0.0";

    public static final int DEFAULT_SERVER_PORT_CONFIG = 5672;

    public static final String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap.servers";

    public static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";

    @Getter
    private String serverHost;

    @Getter
    private int serverPort;

    @Getter
    private String kafkaBootstrapServers;

    /**
     * load the properties
     * @param properties
     */
    public AmqpConfig(Properties properties) {
        this.serverHost = parseString(properties, SERVER_HOST_CONFIG, DEFAULT_SERVER_HOST_CONFIG);
        this.serverPort = parseInt(properties, SERVER_PORT_CONFIG, DEFAULT_SERVER_PORT_CONFIG);
        this.kafkaBootstrapServers = parseString(properties, KAFKA_BOOTSTRAP_SERVERS_CONFIG, DEFAULT_KAFKA_BOOTSTRAP_SERVERS_CONFIG);
    }

    public static AmqpConfig fromProperties(Properties properties) {
        return new AmqpConfig(properties);
    }

    private int parseInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public String parseString(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : value;
    }
}
