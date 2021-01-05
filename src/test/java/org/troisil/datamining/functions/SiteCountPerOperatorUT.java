package org.troisil.datamining.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.TestCase;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SiteCountPerOperatorUT {
    private static final Config testConfig = ConfigFactory.load("application.conf");
    @NonNull
    private static SparkSession sparkSession;

    private static Configuration hadoopConf = new Configuration();

    private static String relativeLocalInputPath = testConfig.getString("app.data.input");

    private static Path inputPath = new Path(relativeLocalInputPath);

    private static FileSystem fs;
    private static FileSystem hdfs;

    @BeforeClass
    public static void setUp() throws IOException {
        log.info("init hdfs");
        fs = FileSystem.getLocal(hadoopConf);
        log.info("fs={}", fs.getScheme());
        hdfs = FileSystem.get(hadoopConf);
        log.info("hdfs={}", hdfs.getScheme());
        clean();
        hdfs.mkdirs(inputPath);
        hdfs.copyFromLocalFile(inputPath, inputPath);
        assertThat(hdfs.exists(inputPath)).isTrue();
        assertThat(hdfs.listFiles(inputPath, true).hasNext()).isTrue();

        log.info("Starting Test SparkSession");
        sparkSession = SparkSession.builder()
                .master(testConfig.getString("app.master"))
                .appName(testConfig.getString("app.name"))
                .getOrCreate();
    }

    private static void clean() throws IOException {
        if(hdfs != null){
            hdfs.delete(inputPath, true);
        }
    }

    @Test
    public void operatorTest() {
        SiteCountPerOperator siteCountPerOperator = new SiteCountPerOperator();
        Dataset<Row> ds = siteCountPerOperator.apply(new DatasetCsvReader(sparkSession, relativeLocalInputPath).get());
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
