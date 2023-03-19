package com.losedream.spark.learn.core;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class AccumulatorVariable {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
        .setAppName("AccumulatorVariable")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 创建Accumulator变量
    // 需要调用SparkContext的accumulator()方法
    Accumulator<Integer> accumulator = sparkContext.accumulator(0);
    List<Integer> numberList = Lists.newArrayList(1, 2, 3, 4, 5);

    JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);
    // 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
    numbers.foreach(accumulator::add);

    // 在driver程序中，可以调用Accumulator的value()方法，获取其值
    System.err.println(accumulator.value());

    sparkContext.close();
  }

}
