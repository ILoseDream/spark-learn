package com.losedream.spark.learn.core.upgrade;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.JavaSourceClassLoader;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class Intersection {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName("Intersection");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // intersection算子
    // 获取两个rdd中，相同的数据
    // 有的公司内，有些人可能同时在做不同的项目，属于不同的项目组
    // 所以要针对代表两个项目组同事的rdd，取出其交集
    List<String> project1MemberList = Arrays.asList("张三", "李四", "王五", "麻子");
    JavaRDD<String> project1MemberRDD = sparkContext.parallelize(project1MemberList);

    List<String> project2MemberList = Arrays.asList("张三", "王五", "小明", "小倩");
    JavaRDD<String> project2MemberRDD = sparkContext.parallelize(project2MemberList);

    JavaRDD<String> intersection = project1MemberRDD.intersection(project2MemberRDD);
    intersection.collect().forEach(System.out::println);

    sparkContext.close();
  }

}
