package com.losedream.spark.learn.core;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 分组取top3
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class GroupTop3 {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
        .setAppName("GroupTop3")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/score.txt");

    JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();
    JavaPairRDD<String, Iterable<Integer>> top3Score = groupedPairs.mapToPair(
        tuple2 -> {
          String className = tuple2._1;
          Iterable<Integer> classScores = tuple2._2;
          List<Integer> list = Lists.newArrayList();
          classScores.iterator().forEachRemaining(list::add);
          list.sort((o1, o2) -> o2 - o1);
          if (list.size() < 3) {
            return new Tuple2<>(className, list);
          } else {
            return new Tuple2<>(className, list.subList(0, 3));
          }
        });

    top3Score.foreach(tuple2 -> {
      System.out.println("class: " + tuple2._1);
      for (Integer score : tuple2._2) {
        System.out.println(score);
      }
      System.out.println("=======================================");
    });

    sparkContext.close();
  }


}
