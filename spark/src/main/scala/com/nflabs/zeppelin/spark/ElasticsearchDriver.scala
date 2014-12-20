package com.nflabs.zeppelin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat

class ElasticsearchDriver {
  def init = {
    val sconf = new SparkConf().setAppName("Test").setMaster("local").set("spark.serializer", classOf[KryoSerializer].getName)
    val sc = new SparkContext(sconf)
    val conf = new Configuration()
    conf.set("es.nodes", "10.101.40.47:11200")
    conf.set("es.resource", "_all/linetv-web")
    conf.set("es.query", "?q=me*")
    sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  }
}
