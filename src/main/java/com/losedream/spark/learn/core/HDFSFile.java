package com.losedream.spark.learn.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/2
 */
public class HDFSFile {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("HDFSFile");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> rdd = sparkContext.textFile("hdfs://spark1:9000/spark.txt");
    JavaRDD<Integer> map =
        rdd.map(
            new Function<String, Integer>() {
              @Override
              public Integer call(String v1) throws Exception {
                return StringUtils.length(v1);
              }
            });

    Integer reduce =
        map.reduce(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
              }
            });

    System.out.println("文件总字数是：" + reduce);

    // 关闭JavaSparkContext
    sparkContext.close();
  }
}
