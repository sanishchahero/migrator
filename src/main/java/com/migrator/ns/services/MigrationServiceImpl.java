package com.migrator.ns.services;

import com.migrator.config.DatabaseConfig;
import com.migrator.ns.dto.MigrationRequest;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class MigrationServiceImpl implements MigrationService {

    private final SparkSession spark;
    private final DatabaseConfig databaseConfig;

    private static boolean hasJoinTable(MigrationRequest migrationRequest) {
        return Objects.nonNull(migrationRequest.getJoinTable());
    }

    private static Dataset<Row> joinMainAndAggreatedDataSet(MigrationRequest migrationRequest, Dataset<Row> mainDataSet, Dataset<Row> aggregatedDF, Dataset<Row> joinDataSet) {
        return mainDataSet
                .join(aggregatedDF, mainDataSet.col(migrationRequest.getSourceTableCommonColumn())
                        .equalTo(joinDataSet.col(migrationRequest.getJoinTableCommonColumn())));
    }

    private static Dataset<Row> aggregatePropertyKeyAndValueColumns(MigrationRequest migrationRequest, Dataset<Row> joinDataSet) {
        return joinDataSet
                .groupBy(migrationRequest.getJoinTableCommonColumn())
                .agg(functions.collect_list(
                                functions.struct(joinDataSet.col("property_key"), joinDataSet.col("property_value")))
                        .alias("properties_map"));
    }

    @Override
    public void proceedMigration(MigrationRequest migrationRequest) {
        Dataset<Row> mysqlData = loadDataFromSourceDB(migrationRequest);
        writeDataToSinkDB(mysqlData, migrationRequest);
    }

    private Dataset<Row> loadDataFromSourceDB(MigrationRequest migrationRequest) {
        if (hasJoinTable(migrationRequest)) {
            return readFromMainTableAndJoinTable(migrationRequest);
        }
        return readFromMainTable(migrationRequest);
    }

    private Dataset<Row> readFromMainTableAndJoinTable(MigrationRequest migrationRequest) {
        Dataset<Row> mainDataSet = readFromMainTable(migrationRequest);
        Dataset<Row> joinDataSet = loadFromJoinTable(migrationRequest);
        Dataset<Row> aggregatedDataSet = aggregatePropertyKeyAndValueColumns(migrationRequest, joinDataSet);
        Dataset<Row> resultDataSet = joinMainAndAggreatedDataSet(migrationRequest, mainDataSet, aggregatedDataSet, joinDataSet);
        return prepareFinalDataSet(resultDataSet);
    }

    private static Dataset<Row> prepareFinalDataSet(Dataset<Row> resultDataSet) {
        return resultDataSet
                .select(resultDataSet.col("receiver"),
                        resultDataSet.col("queued_time"),
                        resultDataSet.col("id"),
                        resultDataSet.col("attachment"),
                        resultDataSet.col("attempts"),
                        resultDataSet.col("execution_time"),
                        resultDataSet.col("finished_time"),
                        resultDataSet.col("last_error"),
                        resultDataSet.col("status"),
                        resultDataSet.col("template_id"),
                        resultDataSet.col("version"),
                        functions.map_from_entries(functions.col("properties_map")).alias("property_value_key"));
    }

    private Dataset<Row> loadFromJoinTable(MigrationRequest migrationRequest) {
        return spark.read()
                .format("jdbc")
                .option("url", databaseConfig.getSource().getUrl())
                .option("dbtable", migrationRequest.getJoinTable())
                .option("user", databaseConfig.getSource().getUsername())
                .option("password", databaseConfig.getSource().getPassword())
                .load();
    }

    private Dataset<Row> readFromMainTable(MigrationRequest migrationRequest) {
        return spark.read().format("jdbc")
                .option("url", databaseConfig.getSource().getUrl()).option("dbtable", migrationRequest.getSourceTable())
                .option("user", databaseConfig.getSource().getUsername())
                .option("password", databaseConfig.getSource().getPassword())
                .load();
    }

    private void writeDataToSinkDB(Dataset<Row> sourceData, MigrationRequest migrationRequest) {
        sourceData.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", databaseConfig.getSink().getKeyspace())
                .option("table", migrationRequest.getSinkTable())
                .mode("append")
                .save();
    }
}