package com.nflabs.zeppelin.spark.driver;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import static org.junit.Assert.*;

public class ElasticsearchDriverTest {
    private ElasticsearchDriver sut = new ElasticsearchDriver();
    @Test
    public void init() {
        JavaPairRDD<Text, MapWritable> esRDD = sut.init();
        System.out.println(esRDD.collect());
    }
}