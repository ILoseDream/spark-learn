package com.losedream.spark.learn.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/23
 */
public class SaveModeTest {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("SaveModeTest");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrame json = sqlContext.read().format("json")
        .load("hdfs://spark1:9000/people.json");

    json.save("hdfs://spark1:9000/people_savemode_test", "json", SaveMode.Append);
  }

}
