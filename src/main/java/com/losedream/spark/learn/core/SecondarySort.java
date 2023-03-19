package com.losedream.spark.learn.core;

import com.losedream.spark.learn.core.sort.SecondarySortKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 二次排序 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class SecondarySort {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
        .setAppName("SecondarySort")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sort.txt");

    JavaPairRDD<SecondarySortKey, String> pairRDD = lines.mapToPair(
        s -> {
          String[] lineSplit = StringUtils.split(s, " ");
          SecondarySortKey secondarySortKey = new SecondarySortKey(Integer.parseInt(lineSplit[0]),
              Integer.parseInt(lineSplit[1]));
          return new Tuple2<>(secondarySortKey, s);
        });

    JavaPairRDD<SecondarySortKey, String> sortRDD = pairRDD.sortByKey();
    JavaRDD<String> sortedLines = sortRDD.map(Tuple2::_2);
    sortedLines.foreach(System.out::println);
    sparkContext.close();
    // 链式调用
//    lines.mapToPair(s -> {
//          String[] split = StringUtils.split(s, " ");
//          return new Tuple2<>(
//              SecondarySortKey.of(Integer.parseInt(split[0]), Integer.parseInt(split[1])), s);
//        })
//        .sortByKey()
//        .map(Tuple2::_2)
//        .foreach(System.out::println);

  }

}
