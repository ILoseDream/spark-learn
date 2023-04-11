package com.losedream.spark.learn.core.upgrade;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class Distinct {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
        .setAppName("Distinct")
        .setMaster("local");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // distinct算子
    // 对rdd中的数据进行去重

    // uv统计案例
    // uv：user view，每天每个用户可能对网站会点击多次
    // 此时，需要对用户进行去重，然后统计出每天有多少个用户访问了网站
    // 而不是所有用户访问了网站多少次（pv）
    List<String> accessLogs = Arrays.asList(
        "user1 2016-01-01 23:58:42",
        "user1 2016-01-01 23:58:43",
        "user1 2016-01-01 23:58:44",
        "user2 2016-01-01 12:58:42",
        "user2 2016-01-01 12:58:46",
        "user3 2016-01-01 12:58:42",
        "user4 2016-01-01 12:58:42",
        "user5 2016-01-01 12:58:42",
        "user6 2016-01-01 12:58:42",
        "user6 2016-01-01 12:58:45");

    JavaRDD<String> accessLogsRDD = sparkContext.parallelize(accessLogs);
    JavaRDD<String> userIdsRDD = accessLogsRDD.map(new Function<String, String>() {
      @Override
      public String call(String accessLog) throws Exception {
        return accessLog.split(" ")[0];
      }
    });

    JavaRDD<String> distinct = userIdsRDD.distinct();
    int uv = distinct.collect().size();
    System.out.println("uv: " + uv);

    sparkContext.close();
  }

}
