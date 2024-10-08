package com.migrator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "database")
public class DatabaseConfig {
    private Source source;
    private Sink sink;

    @Getter
    @Setter
    public static class Source {
        private String url;
        private String username;
        private String password;
    }

    @Getter
    @Setter
    public static class Sink {
        private String keyspace;
    }
}