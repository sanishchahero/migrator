package com.migrator.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder().appName("Mysql to Cassandra Migrator")
                .master("local[*]")
                .config("spark.cassandra.connection.host", "localhost:9042")
                .config("spark.cassandra.connection.port", "9042")
                .config("spark.cassandra.auth.username", "username")
                .config("spark.cassandra.auth.password", "password")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.cores", "4")
                .config("spark.driver.memory", "2g")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.dynamicAllocation.enabled", "true")
                .getOrCreate();
    }
}