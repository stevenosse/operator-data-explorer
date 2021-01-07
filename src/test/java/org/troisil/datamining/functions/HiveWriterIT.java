package org.troisil.datamining.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import org.troisil.datamining.utils.HBaseRow;
import org.troisil.datamining.utils.TestUtil;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HiveWriterIT {
    private static SparkSession sparkSession;

    private final String dbName = "operator";

    private final String tableName = "coordonnees";

    private final String location = "/tmp/hive/warehouse";

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession.builder().master("local[2]").appName("test-writer").getOrCreate();
    }

    @Test
    public void testWriter() {
        List<HBaseRow> expected = TestUtil.buildTestData();

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        log.info("showing databases before...");
        sparkSession.sql("show databases").show();

        new HiveWriter(dbName, tableName, location).accept(expectedData);

        log.info("showing databases after...");
        sparkSession.sql("show databases").show();

        Dataset<HBaseRow> actualData = sparkSession.sql(String.format("SELECT * from %s.%s", dbName, tableName))
                .as(Encoders.bean(HBaseRow.class));

        assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }
}
