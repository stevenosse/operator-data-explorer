package org.troisil.datamining;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DataCsvReaderUT {
    private static final Config testConfig = ConfigFactory.load("application.conf");
    @NonNull
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp(){
        log.info("Starting Test SparkSession");
        sparkSession = SparkSession.builder()
                .master(testConfig.getString("app.master"))
                .appName(testConfig.getString("app.name"))
                .getOrCreate();
    }
    @Test
    public void testReaderEmptyPath(){
        log.info("running test on CSV reader ...");
        String testInputPath = "target/test-classes/data/operator_fake_data.csv";
        DatasetCsvReader reader = new DatasetCsvReader(
                sparkSession, testInputPath
        );
        Dataset<Row> ds = reader.get();
        ds.show(5, false);
        ds.printSchema();
        assertThat(ds.count()).isGreaterThan(0);
    }
}
