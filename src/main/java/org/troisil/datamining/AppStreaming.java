package org.troisil.datamining;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.troisil.datamining.functions.HBaseWriter;
import org.troisil.datamining.functions.KafkaReceiver;

@Slf4j
public class AppStreaming
{
    public static void main( String[] args ) throws InterruptedException {
        log.info( "Data tourism Spark Streaming App" );

        Config config = ConfigFactory.load("application.conf");

        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        log.info("masterUlr={} , appName={}", masterUrl, appName);
        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl).setAppName(appName)
                .set("spark.executor.instances", String.valueOf(config.getInt("app.executor.nb")))
                .set("spark.executor.memory", config.getString("app.executor.memory"))
                .set("spark.executor.cores", config.getString("app.executor.cores"));

        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);

        final JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, new Duration(1000*10));

        KafkaReceiver kafkaReceiver = new KafkaReceiver(jsc);

        JavaDStream<String> stringJavaDStream = kafkaReceiver.get();
        stringJavaDStream.foreachRDD(
                stringJavaRDD -> {
                    if(stringJavaRDD.isEmpty()){
                        log.info("no data received!");
                    } else {
                        Metadata md = new MetadataBuilder().build();
                        StructType schema = new StructType(new StructField[]{
                                new StructField("code_op", DataTypes.IntegerType, true, md),
                                new StructField("nom_op", DataTypes.StringType, true, md),
                                new StructField("nom_dep", DataTypes.StringType, true, md),
                                new StructField("nom_reg", DataTypes.StringType, true, md),
                                new StructField("insee_dep", DataTypes.IntegerType, true, md)
                        }
                        );
                        JavaRDD<Row> rowRDD = stringJavaRDD.map(str -> {
                            String[] fields = str.split(";", 5);
                            return RowFactory.create(Integer.parseInt(fields[0]), fields[1], fields[2], fields[3], Integer.parseInt(fields[4]));
                        });
                        Dataset<Row> raw2 = spark.createDataFrame(rowRDD, schema);

                        Dataset<Row> clean = raw2.filter(raw2.col("code_op").isNotNull());

                        clean.foreach(
                                new ForeachFunction<Row>() {
                                    @Override
                                    public void call(Row s) throws Exception {
                                        log.info("found = {}", s);
                                    }
                                }
                        );
                        new HBaseWriter("op-info-catalog.json").accept(clean);
                    }
                }
        );

        log.info("Done");

        jsc.start();
        jsc.awaitTermination();



    }
}
