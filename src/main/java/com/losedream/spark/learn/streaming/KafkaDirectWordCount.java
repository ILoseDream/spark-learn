package com.losedream.spark.learn.streaming;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class KafkaDirectWordCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("KafkaDirectWordCount");
    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    // 首先，要创建一份kafka参数map
    Map<String, String> kafkaParams = Maps.newHashMap();
    kafkaParams.put("metadata.broker.list",
        "192.168.1.107:9092,192.168.1.108:9092,192.168.1.109:9092");

    // 然后，要创建一个set，里面放入，你要读取的topic
    // 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
    Set<String> topics = Sets.newHashSet();
    topics.add("WordCount");

    JavaPairInputDStream<String, String> lins = KafkaUtils.createDirectStream(
        streamingContext,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topics);

    JavaDStream<String> words = lins.flatMap(
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
