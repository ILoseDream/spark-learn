package com.losedream.spark.learn.core;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 排序的wordcount程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class SortWordCount {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
        .setAppName("SortWordCount")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/spark.txt");
    lines.flatMap(s -> Lists.newArrayList(StringUtils.split(s, " ")))
        .mapToPair(s -> new Tuple2<>(s, 1))
        .reduceByKey(Integer::sum) // 这里统计出每个单词出现的次数
        .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
        .sortByKey(false)
        .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
        .foreach(tuple2 -> System.out.println(tuple2._1 + " appears " + tuple2._2 + " times."));

    sparkContext.close();
  }

}
