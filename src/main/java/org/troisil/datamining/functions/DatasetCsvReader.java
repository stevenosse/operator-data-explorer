package org.troisil.datamining.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class DatasetCsvReader implements Supplier<Dataset<Row>> {
    @NonNull
    final SparkSession sparkSession;

    @NonNull
    final String inputPath;

    @Override
    public Dataset<Row> get() {
        log.info("Reading data from {}", inputPath);
        return sparkSession
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(inputPath);
    }
}
