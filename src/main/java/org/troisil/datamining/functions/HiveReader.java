package org.troisil.datamining.functions;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.troisil.datamining.utils.HBaseRow;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class HiveReader implements Supplier<Dataset<HBaseRow>> {
    private final SparkSession sparkSession;
    private final String dbName;
    private final String tableName;

    @Override
    public Dataset<HBaseRow> get() {
        return sparkSession.sql(String.format("SELECT * from %s.%s", dbName, tableName))
                .as(Encoders.bean(HBaseRow.class));
    }
}
