package com.losedream.spark.learn.sql;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/23
 */
public class ParquetLoadData {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("ParquetLoadData");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    // 读取Parquet文件中的数据，创建一个DataFrame
    DataFrame parquet = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users.parquet");

    // 将DataFrame注册为临时表，然后使用SQL查询需要的数据
    parquet.registerTempTable("users");

    DataFrame nameDataFrame = sqlContext.sql("select name from users");
    JavaRDD<String> userNames = nameDataFrame.javaRDD().map(row -> "Name: " + row.getString(0));

    List<String> collect = userNames.collect();
    collect.forEach(System.out::println);
  }

}
