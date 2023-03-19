package com.losedream.spark.learn.core;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 使用java开发本地测试的 word count 程序
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/1
 */
public class WordCountLocal {

  public static void main(String[] args) {

    // 1 创建 SparkConf 对象 设置Spark应用的配置信息
    SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

    // 2 创建 JavaSparkContext 对象
    // 在 spark 中 SparkContext 是 spark 所有功能的一个入口 无论任何语言 都需要一个 SparkContext
    // SparkContext 的主要作用包括 spark 应用所需要的一些核心组件 包括 调度器 以及去 Spark Master 节点进行注册
    // 但是编写不同的类型的 spark 应用 需要使用不同的 SparkContext
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 3 针对输入源 创建一个 初始的 RDD
    // 输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
    // RDD 有元素的概念 如果是 hdfs 或者本地文件 创建的 RDD 每一个元素相当于文件里的一行
    JavaRDD<String> javaRDD =
        sparkContext.textFile(
            "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/core/spark.txt");

    // 4 针对 初始的 RDD 进行 transformation 操作 也就是一些计算操作
    // 通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
    // function，通常，如果比较简单，则创建指定Function的匿名内部类
    // 但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类

    // 先将每一行拆分成单个的单词
    // FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
    // flatMap 算子的作用 其实就是 将 RDD 的一个元素 给拆分成一个或者多个元素
    JavaRDD<String> words =
        javaRDD.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(StringUtils.split(s, " "));
              }
            });

    // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
    // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
    // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
    // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
    // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
    // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
    JavaPairRDD<String, Integer> pairRDD =
        words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
              }
            });

    // 接着，需要以单词作为key，统计每个单词出现的次数
    // 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
    // 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
    // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
    // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
    // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
    // reduce之后的结果，相当于就是每个单词出现的次数
    JavaPairRDD<String, Integer> reduceRDD =
        pairRDD.reduceByKey(Integer::sum);

    // 到这里为止 我们通过 spark 算子操作 以及统计处单词的次数
    // 一个 spark 应用中 光是 transformation 操作  是不行的 是不会执行的 必须要有一种叫做 action
    reduceRDD.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> tuple2) throws Exception {
            System.out.println(tuple2._1 + "" + " append " + tuple2._2 + " times.");
          }
        });
    sparkContext.close();
  }
}
