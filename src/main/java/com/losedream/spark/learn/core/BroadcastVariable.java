package com.losedream.spark.learn.core;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class BroadcastVariable {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("BroadcastVariable")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
    // 获取的返回结果是Broadcast<T>类型
    final int factor = 3;
    Broadcast<Integer> factorBroadcast = sparkContext.broadcast(factor);
    List<Integer> numberList = Lists.newArrayList(1, 2, 3, 4, 5);
    JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

    // 使用共享变量时，调用其value()方法，即可获取其内部封装的值
    JavaRDD<Integer> multipleNumbers = numbers.map(v1 -> factorBroadcast.getValue() * v1);
    multipleNumbers.foreach(System.out::println);
    sparkContext.close();
  }

}
