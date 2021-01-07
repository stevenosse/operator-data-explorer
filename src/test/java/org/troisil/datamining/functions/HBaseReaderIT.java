package org.troisil.datamining.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseReaderIT {
    private static SparkSession sparkSession;
    private final String catalog = "{\n" +
            "  \"table\": {\n" +
            "    \"namespace\": \"operator\",\n" +
            "    \"name\": \"sitecount\"\n" +
            "  },\n" +
            "  \"rowkey\": \"key\",\n" +
            "  \"columns\": {\n" +
            "    \"key\": {\n" +
            "      \"cf\": \"rowkey\",\n" +
            "      \"col\": \"key\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"operateur\": {\n" +
            "      \"cf\": \"primary\",\n" +
            "      \"col\": \"operateur\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"region\": {\n" +
            "      \"cf\": \"primary\",\n" +
            "      \"col\": \"region\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"nb_sites_2g\": {\n" +
            "      \"cf\": \"cf1\",\n" +
            "      \"col\": \"nb_sites_2g\",\n" +
            "      \"type\": \"int\"\n" +
            "    },\n" +
            "    \"nb_sites_3g\": {\n" +
            "      \"cf\": \"cf1\",\n" +
            "      \"col\": \"nb_sites_3g\",\n" +
            "      \"type\": \"int\"\n" +
            "    },\n" +
            "    \"nb_sites_4g\": {\n" +
            "      \"cf\": \"cf1\",\n" +
            "      \"col\": \"nb_sites_4g\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    private final String catalogName = "target/test-classes/catalog.json";

    @BeforeClass
    public static void setUp() throws IOException {
        sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("test-reader")
                .getOrCreate();
    }
    @Test
    public void testReader() {
        log.info("running hbaseReader test");

        Dataset<HBaseWriterIT.HBaseRow> actualData = new HBaseReader(sparkSession, catalog).get().as(Encoders.bean(HBaseWriterIT.HBaseRow.class));
        actualData.show(2);

        assertThat(actualData.collectAsList()).isEmpty();
    }

}

