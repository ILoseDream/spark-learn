package com.losedream.spark.learn.streaming;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * 基于Kafka receiver方式的实时 word count 程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class KafkaReceiverWordCount {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("KafkaReceiverWordCount");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    // 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
    Map<String, Integer> topicThreadMap = Maps.newHashMap();
    topicThreadMap.put("WordCount", 1);
    JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
        streamingContext,
        "192.168.1.107:2181,192.168.1.108:2181,192.168.1.109:2181",
        "DefaultConsumerGroup",
        topicThreadMap
    );

    JavaDStream<String> words = lines.flatMap(
        new FlatMapFunction<Tuple2<String, String>, String>() {
          @Override
          public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
            return Arrays.asList(tuple2._2.split(" "));
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
