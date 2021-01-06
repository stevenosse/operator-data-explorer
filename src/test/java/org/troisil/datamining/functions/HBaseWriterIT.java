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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterIT {

    private final String catalogName = "target/test-classes/catalog.json";

    private final String catalog = "{\n" +
            "  \"table\": {\n" +
            "    \"namespace\": \"tourism\",\n" +
            "    \"name\": \"coordonnees\"\n" +
            "  },\n" +
            "  \"rowkey\": \"key\",\n" +
            "  \"columns\": {\n" +
            "    \"key\": {\n" +
            "      \"cf\": \"rowkey\",\n" +
            "      \"col\": \"key\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"Latitude\": {\n" +
            "      \"cf\": \"geo\",\n" +
            "      \"col\": \"latitude\",\n" +
            "      \"type\": \"double\"\n" +
            "    },\n" +
            "    \"Longitude\": {\n" +
            "      \"cf\": \"geo\",\n" +
            "      \"col\": \"longitude\",\n" +
            "      \"type\": \"double\"\n" +
            "    },\n" +
            "    \"Adresse_postale\": {\n" +
            "      \"cf\": \"loc\",\n" +
            "      \"col\": \"address\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    @Test
    public void testWriter(){
        log.info("running hbaseWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-reader").getOrCreate();

        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("k1").operateur("orange").region("haute-vienne").nb_sites_2g(1).nb_sites_3g(2).nb_sites_4g(1).build(),
                HBaseRow.builder().key("k2").operateur("bouygues").region("haute-vienne").nb_sites_2g(1).nb_sites_3g(2).nb_sites_4g(1).build()
        );

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        new HBaseWriter(catalogName).accept(expectedData);

        Dataset<HBaseRow> actualData = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load().as(Encoders.bean(HBaseRow.class));

        assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }

    @Data @Builder @AllArgsConstructor @NoArgsConstructor
    public static class HBaseRow implements Serializable {
        private String key;
        private String operateur;
        private String region;
        private Integer nb_sites_2g;
        private Integer nb_sites_3g;
        private Integer nb_sites_4g;
    }
}
