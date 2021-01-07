package org.troisil.datamining.functions;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.io.Serializable;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class HBaseReader implements Supplier<Dataset<Row>> {
    @NonNull
    final SparkSession sparkSession;
    private final String catalog;

    @SneakyThrows
    @Override
    public Dataset<Row> get(){
        return sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load();
    }
    @Data
    @Builder
    @AllArgsConstructor @NoArgsConstructor
    public static class HBaseRow implements Serializable {
        private String key;
        private String operateur;
        private String region;
        private Integer nb_sites_2g;
        private Integer nb_sites_3g;
        private Integer nb_sites_4g;
    }
}
