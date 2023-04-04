package com.losedream.spark.learn.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/23
 */
public class ManuallySpecifyOptions {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("ManuallySpecifyOptions");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrame dataFrame = sqlContext.read().format("json").load("hdfs://spark1:9000/people.json");
    dataFrame.select("name").write().format("parquet")
        .save("hdfs://spark1:9000/peopleName_java");
  }

}
