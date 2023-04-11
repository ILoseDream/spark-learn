package com.losedream.spark.learn.core;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class MapPartitions {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName("MapPartitions");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // 准备一下模拟数据
    List<String> studentNames = Lists.newArrayList("张三", "李四", "王二", "麻子");

    JavaRDD<String> studentNamesRDD = sparkContext.parallelize(studentNames, 2);
    Map<String, Double> studentScoreMap = new HashMap<>();
    studentScoreMap.put("张三", 278.5);
    studentScoreMap.put("李四", 290.0);
    studentScoreMap.put("王二", 301.0);
    studentScoreMap.put("麻子", 205.0);

    // mapPartitions
    // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
    // mapPartitions算子，一次处理一个partition中所有的数据

    // 推荐的使用场景
    // 如果你的RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
    // 但是如果你的RDD的数据量特别大，比如说10亿，不建议用mapPartitions，可能会内存溢出
    JavaRDD<Double> studentScoresRDD = studentNamesRDD.mapPartitions(
        (FlatMapFunction<Iterator<String>, Double>) iterator -> {
          List<Double> studentScoreList = Lists.newArrayList();
          while (iterator.hasNext()) {
            String studentName = iterator.next();
            Double studentScore = studentScoreMap.get(studentName);
            if (Objects.nonNull(studentScore)) {
              studentScoreList.add(studentScore);
            }
          }
          return studentScoreList;
        });

    studentScoresRDD.collect().forEach(System.out::println);
    sparkContext.close();
  }

}
