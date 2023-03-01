package com.losedream.scala.spark.learn.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/1
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("WordCount")
    //    val sc = new SparkContext(conf)
    //    val lines = sc.textFile("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/spark.txt", 1)
    //    val words = lines.flatMap { line => line.split(" ") }
    //    val pairs = words.map { word => (word, 1) }
    //    val wordCounts = pairs.reduceByKey {
    //      _ + _
    //    }
    //    wordCounts.foreach(wordCount => println(wordCount._1 + " append " + wordCount._2 + " times." +
    //      ""))

    val conf = new SparkConf()
      .setAppName("WordCount");
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://spark1:9000/spark.txt", 1);
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey {
      _ + _
    }

    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))

  }

}
