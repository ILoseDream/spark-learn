package com.losedream.spark.learn.core;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/2
 */
public class ParallelizeCollection {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类，的parallelize()方法
    List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    JavaRDD<Integer> rdd = sparkContext.parallelize(numbers);

    Integer reduce =
        rdd.reduce(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
              }
            });

    // 输出累加的和
    System.out.println("1到10的累加和：" + reduce);

    // 关闭JavaSparkContext
    sparkContext.close();
  }
}
