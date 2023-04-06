package com.losedream.spark.learn.streaming;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class HDFSWordCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("HDFSWordCount");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(1));

    // 首先，使用JavaStreamingContext的textFileStream()方法，针对HDFS目录创建输入数据流
    JavaDStream<String> lines = streamingContext.textFileStream(
        "hdfs://spark1:9000/wordcount_dir");

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        String[] split = s.split(" ");
        return Arrays.asList(split);
      }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s, 1);
          }
        });

    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        });

    wordCounts.print();

    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.close();
  }

}
