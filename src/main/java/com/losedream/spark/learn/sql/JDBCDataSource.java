package com.losedream.spark.learn.sql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/4
 */
public class JDBCDataSource {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
        .setAppName("JDBCDataSource");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    // 总结一下 jdbc数据源
    // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
    // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
    // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

    Map<String, String> options = Maps.newHashMap();
    options.put("url", "jdbc:mysql://spark1:3306/testdb");
    options.put("dbtable", "student_infos");

    DataFrame studentInfosDF = sqlContext.read().format("jdbc")
        .options(options).load();

    options.put("dbtable", "student_scores");
    DataFrame studentScoresDF = sqlContext.read().format("jdbc")
        .options(options).load();

    JavaRDD<Row> studentInfosRDD = studentInfosDF.javaRDD();
    JavaPairRDD<String, Integer> studentInfoPairRDD = studentInfosRDD.mapToPair(
        row -> new Tuple2<>(
            row.getString(0),
            Integer.valueOf(String.valueOf(row.getString(1))))
    );

    JavaRDD<Row> studentScoresRDD = studentScoresDF.javaRDD();
    JavaPairRDD<String, Integer> studentScoresPariRDD = studentScoresRDD.mapToPair(
        row -> new Tuple2<>(
            String.valueOf(row.getString(0)),
            Integer.valueOf(String.valueOf(row.get(1))))
    );

    // 将两个DataFrame转换为JavaPairRDD，执行join操作
    JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfoPairRDD.join(
        studentScoresPariRDD);

    // 将JavaPairRDD转换为JavaRDD<Row>
    JavaRDD<Row> studentRowsRDD = studentsRDD.map(
        v1 -> RowFactory.create(v1._1, v1._2._1, v1._2._2));

    // 过滤出分数大于80分的数据
    JavaRDD<Row> filteredStudentRowsRDD = studentRowsRDD.filter(v1 -> v1.getInt(2) > 80);

    // 转换为DataFrame
    List<StructField> structFields = Lists.newArrayList();
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
    StructType structType = DataTypes.createStructType(structFields);
    DataFrame studentsDF = sqlContext.createDataFrame(filteredStudentRowsRDD, structType);

    Row[] rows = studentsDF.collect();
    for (Row row : rows) {
      System.err.println(row);
    }

    // 将DataFrame中的数据保存到mysql表中
    // 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
    studentsDF.javaRDD().foreach(new VoidFunction<Row>() {
      @Override
      public void call(Row row) throws Exception {
        String sql = "insert into good_student_infos values("
            + "'" + String.valueOf(row.getString(0)) + "',"
            + Integer.valueOf(String.valueOf(row.get(1))) + ","
            + Integer.valueOf(String.valueOf(row.get(2))) + ")";

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement stmt = null;
        try {
          conn = DriverManager.getConnection(
              "jdbc:mysql://spark1:3306/testdb", "", "");
          stmt = conn.createStatement();
          stmt.executeUpdate(sql);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (stmt != null) {
            stmt.close();
          }
          if (conn != null) {
            conn.close();
          }
        }
      }
    });

    sparkContext.close();
  }

}
