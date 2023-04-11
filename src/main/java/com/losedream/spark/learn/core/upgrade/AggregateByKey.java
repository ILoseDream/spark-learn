package com.losedream.spark.learn.core.upgrade;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class AggregateByKey {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setAppName("AggregateByKey")
        .setMaster("local");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> lines = sparkContext.textFile(
        "C://Users//Administrator//Desktop//hello.txt",
        3);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String line) throws Exception {
        return Arrays.asList(StringUtils.split(line, " "));
      }
    });

    JavaPairRDD<String, Integer> pairs = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String word) throws Exception {
            return new Tuple2<>(word, 1);
          }
        });

    // aggregateByKey，分为三个参数
    // reduceByKey认为是aggregateByKey的简化版
    // aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
    // 就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
    // 然后才是对所有partition中的数据进行全局聚合

    // 第一个参数是，每个key的初始值
    // 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
    // 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合
    JavaPairRDD<String, Integer> wordCounts = pairs.aggregateByKey(0,
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        },
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        });

    wordCounts.collect().forEach(System.out::println);

    sparkContext.close();
  }

}
