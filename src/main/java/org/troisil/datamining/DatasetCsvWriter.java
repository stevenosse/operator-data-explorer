package org.troisil.datamining;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class DatasetCsvWriter<Row> implements Consumer<Dataset<Row>> {
    private final String outputPathStr;

    @Override
    public void accept(Dataset<Row> tDataset) {
        Dataset<Row> ds = tDataset.cache();
        log.info("Writing data ds.count()={} into outputPathStr = {}", ds.count(), outputPathStr);
        ds.printSchema();
        ds.show(5);
        ds.write().mode(SaveMode.ErrorIfExists)
                .option("header", true)
                .format("csv")
                .save(outputPathStr);
        ds.unpersist();
    }
}
