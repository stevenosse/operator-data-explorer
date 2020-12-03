package org.troisil.datamining;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class DatasetCsvReader implements Supplier<Dataset<Row>> {
    @NonNull
    final SparkSession sparkSession;

    @NonNull
    final String inputPath;

    @Override
    public Dataset<Row> get() {
        return sparkSession
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(inputPath);
    }
}
