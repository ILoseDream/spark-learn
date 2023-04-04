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
public class GenericLoadSave {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("GenericLoadSave");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrame usersDF = sqlContext.read().load("hdfs://spark1:9000/users.parquet");
    usersDF.select("name", "favorite_color")
        .write()
        .save("hdfs://spark1:9000/namesAndFavColors.parquet");
  }


}
