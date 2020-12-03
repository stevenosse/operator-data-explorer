package org.troisil.datamining;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{
    public static void main( String[] args )
    {
        Config config = ConfigFactory.load("application.conf");

        SparkConf sparkConf = new SparkConf()
                .setMaster(config.getString("app.master"))
                .set("executor.cores", config.getString("app.executor.cores"))
                .set("executor.memory", config.getString("app.executor.memory"))
                .set("executor.instances", config.getString("app.executor.instances"));

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();


        log.info("See! It's logging.");
    }
}
