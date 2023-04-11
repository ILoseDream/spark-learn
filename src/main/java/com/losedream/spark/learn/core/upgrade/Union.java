package com.losedream.spark.learn.core.upgrade;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class Union {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setAppName("Union")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // union算子
    // 将两个RDD的数据，合并为一个RDD
    List<String> department1StaffList = Arrays.asList("张三", "李四", "王二", "麻子");
    JavaRDD<String> department1StaffRDD = sparkContext.parallelize(department1StaffList);

    List<String> department2StaffList = Arrays.asList("赵六", "王五", "小明", "小倩");
    JavaRDD<String> department2StaffRDD = sparkContext.parallelize(department2StaffList);
    JavaRDD<String> departmentStaffRDD = department1StaffRDD.union(department2StaffRDD);
    departmentStaffRDD.collect().forEach(System.out::println);

    sparkContext.close();
  }

}
