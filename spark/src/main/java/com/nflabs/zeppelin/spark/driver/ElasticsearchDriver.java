package com.nflabs.zeppelin.spark.driver;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.elasticsearch.hadoop.mr.EsInputFormat;


public class ElasticsearchDriver {
  public JavaPairRDD<Text, MapWritable> init() {
    SparkConf sparkConf = new SparkConf()
        .setAppName("Test")
        .setMaster("local")
        .set("spark.serializer", KryoSerializer.class.getName());
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    Configuration conf = new Configuration();
    conf.set("es.nodes", "10.101.40.47:11200");
    conf.set("es.resource", "_all/linetv-web");
    conf.set("es.query", "?q=me*");
    JavaPairRDD<Text, MapWritable> rdd = jsc.newAPIHadoopRDD(conf,
        EsInputFormat.class,
        Text.class,
        MapWritable.class);
    return rdd;
  }
}
