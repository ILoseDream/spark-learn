package com.losedream.spark.learn.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class Persist {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("Persist")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // cache()或者persist()的使用，是有规则的
    // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
    // 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
    // 而且，会报错，大量的文件会丢失
    JavaRDD<String> lines = sparkContext.textFile(
            "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/spark.txt")
        .cache();

    long startTime = System.currentTimeMillis();
    long count = lines.count();
    System.err.println(count);

    long endTime = System.currentTimeMillis();
    System.out.println("cost " + (endTime - startTime) + " milliseconds.");

    startTime = System.currentTimeMillis();
    count = lines.count();
    System.out.println(count);

    endTime = System.currentTimeMillis();
    System.out.println("cost " + (endTime - startTime) + " milliseconds.");
    sparkContext.close();

    sparkContext.close();
  }

}
