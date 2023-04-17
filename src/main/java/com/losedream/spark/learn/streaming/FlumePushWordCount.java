package com.losedream.spark.learn.streaming;

import com.google.common.collect.Lists;
import java.util.Stack;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

/**
 * 基于Flume Push方式的实时 WordCount 程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/17
 */
public class FlumePushWordCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setAppName("FlumePushWordCount")
        .setMaster("local[2]");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(5));
    JavaReceiverInputDStream<SparkFlumeEvent> lines = FlumeUtils.createStream(streamingContext,
        "192.168.0.103", 8888);

    JavaDStream<String> words = lines.flatMap(
        new FlatMapFunction<SparkFlumeEvent, String>() {
          @Override
          public Iterable<String> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
            byte[] array = sparkFlumeEvent.event().getBody().array();
            String s = new String(array);
            return Lists.newArrayList(StringUtils.split(s, " "));
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
