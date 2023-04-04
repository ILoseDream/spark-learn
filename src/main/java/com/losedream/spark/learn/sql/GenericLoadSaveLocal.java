package com.losedream.spark.learn.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/23
 */
public class GenericLoadSaveLocal {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("GenericLoadSaveLocal")
        .setMaster("local")
        ;
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrame usersDF = sqlContext.read()
        .load("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/users.parquet");
    usersDF.select("name", "favorite_color")
        .write()
        .save("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/namesAndFavColors.parquet");
  }


}
