package org.troisil.datamining.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.Test;
import org.troisil.datamining.utils.HBaseRow;
import org.troisil.datamining.utils.TestUtil;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterIT {

    private final String catalogName = "target/test-classes/catalog.json";

    private String catalog = Files.readAllLines(Paths.get(catalogName)).stream().collect(Collectors.joining(" "));

    public HBaseWriterIT() throws IOException {
    }

    @Test
    public void testWriter(){
        log.info("running hbaseWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer").getOrCreate();

        List<HBaseRow> expected = TestUtil.buildTestData();

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);

        Dataset<HBaseRow> actualData = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load().as(Encoders.bean(HBaseRow.class));

        assertThat(actualData.collectAsList()).isNotEmpty();
        assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }

}
