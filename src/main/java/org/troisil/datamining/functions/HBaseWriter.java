package org.troisil.datamining.functions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
@Slf4j
@RequiredArgsConstructor
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogName;
    @Override
    public void accept(Dataset<Row> rowDataset) {
        log.info("Reading catalog file={}", catalogName);
        try {
            String catalog = String.join("", Files.readAllLines(Paths.get(catalogName), Charset.defaultCharset()));
            log.info("catalog={}", catalog);
            log.info("writing data into hbase...");
            rowDataset
                    .write()
                    .mode("overwrite")
                    .option(HBaseTableCatalog.tableCatalog(), catalog).option(HBaseTableCatalog.newTable(), "5")
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .save();
            log.info("done!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
