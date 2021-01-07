package org.troisil.datamining.functions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class HiveWriter implements Consumer<Dataset<Row>> {
    private final String dbName;
    private final String tableName;
    private final String location;
    //private final String schema;

    @Override
    public void accept(Dataset<Row> rowDataset) {
        String fullTableName = String.format("%s.%s", dbName, tableName);
        String fullDbDataPathStr = String.format("%s/%s.db", location, dbName);

        try {
            SparkSession sparkSession = rowDataset.sparkSession();

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
            rowDataset
                    .write()
                    .mode(SaveMode.Append)
                    .saveAsTable(fullTableName);

        } catch (IOException ioException){
            log.error("could not create write data into hive due to ...", ioException);
        }
        log.info("done!");
    }
}
