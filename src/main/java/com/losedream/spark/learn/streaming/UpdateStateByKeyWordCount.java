package com.losedream.spark.learn.streaming;

import com.google.common.base.Optional;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 基于updateStateByKey算子实现缓存机制的实时 word count 程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class UpdateStateByKeyWordCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("UpdateStateByKeyWordCount");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，
    // 开启checkpoint机制 这样的话才能把每个key对应的state除了在内存中有，
    // 那么是不是也要checkpoint一份 因为你要长期保存一份key的state的话，
    // 那么spark streaming是要求必须用checkpoint的，以便于在 内存数据丢失的时候，
    // 可以从checkpoint中恢复数据
    streamingContext.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");

    JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost",
        9999);
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

    // 到了这里，就不一样了，之前的话，是不是直接就是pairs.reduceByKey
    // 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
    // 然后，可以打印出那个时间段的单词计数
    // 但是，有个问题，你如果要统计每个单词的全局的计数呢？
    // 就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现
    // 就必须基于redis这种缓存，或者是mysql这种db，来实现累加
    JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
        (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {
          // 首先定义一个全局的单词计数
          Integer newValue = 0;

          // 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
          // 如果存在，说明这个key之前已经统计过全局的次数了
          if (state.isPresent()) {
            newValue = state.get();
          }
          // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计次数
          for (Integer value : values) {
            newValue += value;
          }

          return Optional.of(newValue);
        });

    // 到这里为止，相当于是，每个batch过来是，计算到pairs DStream，
    // 就会执行全局的updateStateByKey 算子，
    // updateStateByKey返回的JavaPairDStream，
    // 其实就代表了每个key的全局的计数打印出来
    wordCounts.print();

    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.close();
  }

}
