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
import org.junit.Test;
import org.troisil.datamining.utils.HBaseRow;
import org.troisil.datamining.utils.TestUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterIT {

    private final String catalogName = "target/test-classes/catalog.json";

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

    @Test
    public void testWriter(){
        log.info("running hbaseWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer").getOrCreate();

        List<HBaseRow> expected = TestUtil.buildTestData();

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);

        Dataset<HBaseRow> actualData = new HBaseReader(sparkSession, catalog).get().as(Encoders.bean(HBaseRow.class));

        assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }

}
