package com.losedream.spark.learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 取最大的前3个数字
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class Top3 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("Top3")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/top.txt");

    lines.mapToPair(s -> new Tuple2<>(Integer.valueOf(s), s))
        .sortByKey(false)
        .map(Tuple2::_1)
        .take(3)// 取前三条
        .forEach(System.out::println);
    sparkContext.close();
  }

}
