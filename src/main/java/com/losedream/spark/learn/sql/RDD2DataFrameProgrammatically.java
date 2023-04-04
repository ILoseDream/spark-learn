package com.losedream.spark.learn.sql;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/23
 */
public class RDD2DataFrameProgrammatically {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf()
        .setAppName("RDD2DataFrameProgrammatically")
        .setMaster("local");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    SQLContext sqlContext = new SQLContext(sparkContext);

    // 第一步 构建普通 RDD
    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/students.txt");

    JavaRDD<Row> map = lines.map(line -> {
      String[] split = StringUtils.split(line, ",");
      return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
    });

    // 第二步，动态构造元数据
    // 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
    // 或者是配置文件中，加载出来的，是不固定的
    // 所以特别适合用这种编程的方式，来构造元数据
    List<StructField> structFields = Lists.newArrayList();
    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    StructType structType = DataTypes.createStructType(structFields);

    // 第三步，使用动态构造的元数据，将RDD转换为DataFrame
    DataFrame dataFrame = sqlContext.createDataFrame(map, structType);
    dataFrame.registerTempTable("students");

    DataFrame teenagerDF = sqlContext.sql("select * from students where age<=18");
    List<Row> rows = teenagerDF.javaRDD().collect();
    for (Row row : rows) {
      System.out.println(row);
    }

  }

}
