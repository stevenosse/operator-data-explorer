package org.troisil.datamining.functions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class HDFSReader implements Supplier<Dataset<Row>> {
    private final SparkSession sparkSession;
    private final String inputPathStr;

    @Override
    public Dataset<Row> get() {
        log.info("reading data from inputPathStr={}", inputPathStr);
        try {
            FileSystem hdfs = FileSystem.get(new Path(inputPathStr).toUri(), sparkSession.sparkContext().hadoopConfiguration());
            if (hdfs.exists(new Path(inputPathStr))) {
                return sparkSession.read()
                        .option("header", true)
                        .option("delimiter", ",")
                        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                        .csv(inputPathStr);
            }
        }catch (IOException ioException){
            log.error("could not read data from inputPathStr={}", inputPathStr);
        }
        return sparkSession.emptyDataset(Encoders.bean(Row.class));
    }
}
