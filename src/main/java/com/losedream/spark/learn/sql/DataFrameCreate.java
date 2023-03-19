package com.losedream.spark.learn.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class DataFrameCreate {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("DataFrameCreate")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    SQLContext sqlContext = new SQLContext(sparkContext);
    DataFrame json = sqlContext.read()
        .json("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/students.json");
    json.show();

    sparkContext.close();
  }

}
