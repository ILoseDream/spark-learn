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
public class SaveModeTestLocal {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("SaveModeTest")
        .setMaster("local")
        ;

    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrame json = sqlContext.read().format("json")
        .load("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/people.json");

    json.save("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/people_savemode_test", "json", SaveMode.Append);
  }

}
