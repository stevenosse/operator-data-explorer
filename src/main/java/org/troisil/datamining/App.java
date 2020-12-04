package org.troisil.datamining;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.troisil.datamining.functions.DatasetCsvReader;
import org.troisil.datamining.functions.OperatorSiteCoordinatesFunction;
import org.troisil.datamining.functions.SiteCountPerOperator;

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

        log.info("Starting Spark session");
        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        var datasetCsvReader = new DatasetCsvReader(sparkSession, config.getString("app.data.input"));

        /*var ds = datasetCsvReader.get();
        OperatorSiteCoordinatesFunction siteCoordinatesFunction = new OperatorSiteCoordinatesFunction("Orange");
        Dataset<Row> filtered = siteCoordinatesFunction.apply(ds);

        filtered.show(2, false);

        filtered.printSchema();*/

        /**
         * Récupérer le nombre de site 2G, 3G, 4G par région de chaque opérateur
         */
        SiteCountPerOperator siteCountPerOperator = new SiteCountPerOperator();
        var ds = siteCountPerOperator.apply(datasetCsvReader.get());

        ds.show(2);
        ds.printSchema();

    }
}
