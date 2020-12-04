package org.troisil.datamining.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.TestCase;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SiteCountPerOperatorUT {
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
    public void operatorTest() {
        SiteCountPerOperator siteCountPerOperator = new SiteCountPerOperator();
        Dataset<Row> ds = siteCountPerOperator.apply(new DatasetCsvReader(sparkSession, testConfig.getString("app.data.input")).get());
        ds.show(2);

        assertThat(Arrays.stream(ds.schema().fieldNames()).count()).isEqualTo(5);
        assertThat(Arrays.stream(ds.schema().fieldNames()).anyMatch(s -> {
            return s.equalsIgnoreCase("operateur") ||
                    s.equalsIgnoreCase("region") ||
                    s.equalsIgnoreCase("nb_sites_2g") ||
                    s.equalsIgnoreCase("nb_sites_3g") ||
                    s.equalsIgnoreCase("nb_sites_4g");
        })).isTrue();
        assertThat(ds.first().get(0)).isEqualTo("Orange");
    }
}
