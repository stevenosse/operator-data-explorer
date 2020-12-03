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
public class DatasetCsvWriterUT {
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
    public void testWriteCsvFile(){
        log.info("running test on CSV Writer ...");
        String testOutputPath = "target/test-classes/data/operator_fake_data_out.csv";
        String testInputPath = "target/test-classes/data/operator_fake_data.csv";
        DatasetCsvWriter writer = new DatasetCsvWriter(testOutputPath);

        DatasetCsvReader reader = new DatasetCsvReader(
                sparkSession, testInputPath
        );
        Dataset<Row> dt = reader.get();
        Dataset<Row> ds = writer.accept(dt);

        ds.show(5, false);
        ds.printSchema();
        assertThat(ds.count()).isGreaterThan(0);
    }
}
