package com.losedream.spark.learn.streaming;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 基于transform的实时广告计费日志黑名单过滤
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class TransformBlacklist {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("TransformBlacklist");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    // 用户对我们的网站上的广告可以进行点击
    // 点击之后，是不是要进行实时计费，点一下，算一次钱
    // 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
    // 只要是黑名单中的用户点击的广告，我们就给过滤掉

    // 先做一份模拟的黑名单RDD
    List<Tuple2<String, Boolean>> blackList = Lists.newArrayList();
    blackList.add(new Tuple2<String, Boolean>("tom", true));
    final JavaRDD<Tuple2<String, Boolean>> blacklistRDD = streamingContext.sc()
        .parallelize(blackList);

    // 这里的日志格式，就简化一下，就是date username的方式
    JavaReceiverInputDStream<String> adsClickLogDStream = streamingContext.socketTextStream(
        "spark1", 9999);

    // 所以，要先对输入的数据，进行一下转换操作，变成，(username, date username)
    // 以便于，后面对每个batch RDD，与定义好的黑名单RDD进行join操作
    JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(
        new PairFunction<String, String, String>() {
          @Override
          public Tuple2<String, String> call(String adsClickLog) throws Exception {
            return new Tuple2<>(adsClickLog.split(" ")[1], adsClickLog);
          }
        });

    // 然后，就可以执行transform操作了，将每个batch的RDD，
    // 与黑名单RDD进行join、filter、map等操作实时进行黑名单过滤
    JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(
        new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
          @Override
          public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD)
              throws Exception {
            // 这里为什么用左外连接？
            // 因为，并不是每个用户都存在于黑名单中的
            // 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到
            // 就给丢弃掉了
            // 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到
            // 也还是会被保存下来的
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
                userAdsClickLogRDD.leftOuterJoin(blacklistRDD);

            // 连接之后，执行filter算子
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                  @Override
                  public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2)
                      throws Exception {
                    return !tuple2._2._2.isPresent() || !tuple2._2._2.get();
                  }
                });

            // 此时，filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
            // 进行map操作，转换成我们想要的格式
            return filteredRDD.map(
                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                  @Override
                  public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) {
                    return v1._2._1;
                  }
                });
          }
        });

    // 打印有效的广告点击日志
    // 其实在真实企业场景中，这里后面就可以走写入kafka、ActiveMQ等这种中间件消息队列
    // 然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
    validAdsClickLogDStream.print();

    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.close();
  }

}
