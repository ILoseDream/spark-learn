package com.losedream.spark.learn.streaming;

import com.google.common.base.Optional;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 基于持久化机制的实时 wordcount 程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class PersistWordCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("PersistWordCount");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    streamingContext.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");

    JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("spark1", 9999);

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" "));
      }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s, 1);
          }
        });

    JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

          @Override
          public Optional<Integer> call(List<Integer> values, Optional<Integer> state)
              throws Exception {
            Integer newValue = 0;

            if (state.isPresent()) {
              newValue = state.get();
            }

            for (Integer value : values) {
              newValue += value;
            }

            return Optional.of(newValue);
          }
        });

    wordCounts.foreach(new Function<JavaPairRDD<String, Integer>, Void>() {
      @Override
      public Void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
        wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
          @Override
          public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
            // 给每个partition，获取一个连接
            Connection connection = ConnectionPool.getConnection();
            // 遍历partition中的数据，使用一个连接，插入数据库
            Tuple2<String, Integer> wordCount = null;
            while (wordCounts.hasNext()) {
              wordCount = wordCounts.next();
              String sql = "insert into wordcount(word,count) "
                  + "values('" + wordCount._1 + "'," + wordCount._2 + ")";

              Statement stmt = connection.createStatement();
              stmt.executeUpdate(sql);
            }

            // 用完以后，将连接还回去
            ConnectionPool.returnConnection(connection);
          }
        });
        return null;
      }
    });

    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.close();
  }

}
