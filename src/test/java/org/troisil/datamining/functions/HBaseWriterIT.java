package org.troisil.datamining.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterIT {
    private static final Config testConfig = ConfigFactory.load("application.conf");
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() throws IOException {
        sparkSession = SparkSession.builder()
                .master(testConfig.getString("app.master"))
                .appName(testConfig.getString("app.name"))
                .getOrCreate();
    }
    @Test
    public void testWriter() {
        var datasetCsvReader = new DatasetCsvReader(sparkSession, testConfig.getString("app.data.input"));
        SiteCountPerOperator siteCountPerOperator = new SiteCountPerOperator();
        var ds = siteCountPerOperator.apply(datasetCsvReader.get());
        ds.printSchema();
        var writer = new HBaseWriter("target/test-classes/catalog.json");
        writer.accept(ds);
    }
}
