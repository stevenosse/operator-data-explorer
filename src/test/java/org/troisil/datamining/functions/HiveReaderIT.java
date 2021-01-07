package org.troisil.datamining.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.troisil.datamining.utils.HBaseRow;
import org.troisil.datamining.utils.TestUtil;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HiveReaderIT {
    private static final  String dbName = "operator";
    private static final String tableName = "site_count_per_operator";
    private static final String location = "/tmp/hive/warehouse";

    public static SparkSession sparkSession;
    public static List<HBaseRow> expectedData = TestUtil.buildTestData();

    @BeforeClass
    public static void setUp() {
        String fullTableName = String.format("%s.%s", dbName, tableName);
        String fullDbDataPathStr = String.format("%s/%s.db", location, dbName);

        try {
            sparkSession = SparkSession.builder().master("local[2]").appName("test-reader")
                    .enableHiveSupport()
                    .getOrCreate();

            FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());

            Path fullDbPath = new Path(fullDbDataPathStr);
            if(hdfs.exists(fullDbPath)) {
                hdfs.delete(fullDbPath, true);
            }
            hdfs.mkdirs(fullDbPath);
            String createDbQuery = String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '%s'", dbName, fullDbDataPathStr);
            log.info("creating database using createDbQuery={}...", createDbQuery);
            sparkSession.sql(createDbQuery);

            log.info("writing data into hive table = {}...", fullTableName);
            Dataset<Row> expectedDataset = sparkSession.createDataset(expectedData, Encoders.bean(HBaseRow.class)).toDF();
            expectedDataset
                    .write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(fullTableName);

        } catch (IOException ioException){
            log.error("Failed writing data to hive, reason : ...", ioException);
        }
    }

    @Test
    public void testReader() {
        Dataset<HBaseRow> data = new HiveReader(sparkSession, dbName, tableName).get();

        assertThat(data.collectAsList()).isNotEmpty();
        assertThat(data.collectAsList()).containsExactlyInAnyOrder(expectedData.toArray(new HBaseRow[0]));
    }
}
