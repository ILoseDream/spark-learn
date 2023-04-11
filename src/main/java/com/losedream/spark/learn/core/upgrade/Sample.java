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
public class Sample {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
        .setAppName("Sample")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
        "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
    JavaRDD<String> staffRDD = sparkContext.parallelize(staffList);

    // sample算子
    // 可以使用指定的比例，比如说0.1或者0.9，从RDD中随机抽取10%或者90%的数据
    // 从RDD中随机抽取数据的功能
    // 推荐不要设置第三个参数，seed
    JavaRDD<String> luckyStaffRDD = staffRDD.sample(false, 0.5);
    luckyStaffRDD.collect().forEach(System.out::println);

    sparkContext.close();
  }

}
