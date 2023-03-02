package com.losedream.spark.learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/2
 */
public class LineCount {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> rdd =
        sparkContext.textFile(
            "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/spark.txt");

    JavaPairRDD<String, Integer> pairRDD =
        rdd.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
              }
            });

    JavaPairRDD<String, Integer> lineCounts =
        pairRDD.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
              }
            });

    // 执行一个action操作，foreach，打印出每一行出现的次数
    lineCounts.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {

          private static final long serialVersionUID = 1L;

          @Override
          public void call(Tuple2<String, Integer> t) throws Exception {
            System.out.println(t._1 + " appears " + t._2 + " times.");
          }
        });

    // 关闭JavaSparkContext
    sparkContext.close();
  }
}
