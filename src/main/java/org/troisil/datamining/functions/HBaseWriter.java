package org.troisil.datamining.functions;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogName;

    @SneakyThrows
    @Override
    public void accept(Dataset<Row> rowDataset) {
        log.info("Reading catalog file {}", catalogName);

        String catalog = Files.readAllLines(Paths.get(catalogName)).stream().collect(Collectors.joining(" "));

        rowDataset.write().option(HBaseTableCatalog.tableCatalog(), catalog)
                .option(HBaseTableCatalog.newTable(), "5")
                .mode("overwrite")
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }
}
