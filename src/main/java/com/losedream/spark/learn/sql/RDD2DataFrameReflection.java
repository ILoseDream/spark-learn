package com.losedream.spark.learn.sql;

import com.losedream.spark.learn.sql.model.Student;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用反射的方式将RDD转换为DataFrame
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class RDD2DataFrameReflection {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("RDD2DataFrameReflection")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    SQLContext sqlContext = new SQLContext(sparkContext);

    JavaRDD<String> lines = sparkContext.textFile(
        "/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/sql/students.txt");

    JavaRDD<Student> rdd = lines.map(RDD2DataFrameReflection::string2Student);

    // 使用反射方式，将RDD转换为DataFrame
    // Student.class传入进去，其实就是用反射的方式来创建DataFrame
    // 因为Student.class本身就是反射的一个应用
    // 然后底层还得通过对Student Class进行反射，来获取其中的field
    // 这里要求，JavaBean必须实现Serializable接口，是可序列化的
    DataFrame dataFrame = sqlContext.createDataFrame(rdd, Student.class);

    // 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
    dataFrame.registerTempTable("students");

    // 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生
    DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

    // 将查询出来的DataFrame，再次转换为RDD
    JavaRDD<Row> rowJavaRDD = teenagerDF.javaRDD();

    // 将RDD中的数据，进行映射，映射为Student
    JavaRDD<Student> teenagerStudentRDD = rowJavaRDD.map(row -> {
      // row中的数据的顺序，可能是跟我们期望的是不一样的！
      Student stu = new Student();
      stu.setId(row.getInt(1));
      stu.setName(row.getString(2));
      stu.setAge(row.getInt(0));
      return stu;
    });

    teenagerStudentRDD.collect().forEach(System.out::println);
    sparkContext.close();
  }

  private static Student string2Student(String v1) {
    String[] split = StringUtils.split(v1, ",");
    Student student = new Student();
    student.setId(Integer.parseInt(split[0]));
    student.setName(String.valueOf(split[1]));
    student.setAge(Integer.parseInt(split[2]));
    return student;
  }
}
