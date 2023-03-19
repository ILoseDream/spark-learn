package com.losedream.spark.learn.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * DataFrame的常用操作
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class DataFrameOperation {

  public static void main(String[] args) {
    // 创建 DataFrame
    SparkConf conf = new SparkConf()
        .setAppName("DataFrameOperation")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    // 创建出来的DataFrame完全可以理解为一张表
    DataFrame dataFrame = sqlContext.read()
        .json("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/students.json");

    // 打印DataFrame中所有的数据（select * from ...）
    dataFrame.show();

    // 打印DataFrame的元数据（Schema）
    dataFrame.printSchema();

    // 查询某列所有的数据
    dataFrame.select("name").show();

    // 查询某列所有的数据
    dataFrame.select(
        dataFrame.col("name"),
        dataFrame.col("age").plus(1).alias("plusOne")
    ).show();

    // 根据某一列的值进行过滤
    dataFrame.filter(dataFrame.col("age").gt(18)).show();

    // 根据某一列进行分组，然后进行聚合
    dataFrame.groupBy(dataFrame.col("age")).count().show();

    sparkContext.close();
  }

}
