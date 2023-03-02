package com.losedream.spark.learn.core;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * transformation操作实战
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/2
 */
public class TransformationOperation {

  public static void main(String[] args) {
    map();
  }

  /** map算子案例：将集合中每一个元素都乘以2 */
  private static void map() {
    SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    // 构造集合
    List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

    // 并行化集合 创建初始 RDD
    JavaRDD<Integer> rdd = sparkContext.parallelize(numbers);

    // 使用 map 算子 将集合中的每个元素乘以2
    // map 算子 对任何类型的 rdd 都是可以调用的
    // Function 需要两个范型 第一个范型为上一个算子的结果 第二个范型为 Function 算子 处理的 返回类型
    // Function 算子计算出来的结果 会组成一个 新的 RDD
    JavaRDD<Integer> mapRdd =
        rdd.map(
            new Function<Integer, Integer>() {
              @Override
              public Integer call(Integer v1) throws Exception {
                return v1 * 2;
              }
            });

    mapRdd.foreach(
        new VoidFunction<Integer>() {
          @Override
          public void call(Integer integer) throws Exception {
            System.out.println(integer);
          }
        });

    sparkContext.close();
  }

  /** filter算子案例：过滤集合中的偶数 */
  private static void filter() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 模拟集合
    List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // 并行化集合，创建初始RDD
    JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

    // 对初始RDD执行filter算子，过滤出其中的偶数
    // 如果你想在新的RDD中保留这个元素，那么就返回true；否则，不想保留这个元素，返回false
    JavaRDD<Integer> filter =
        numberRDD.filter(
            new Function<Integer, Boolean>() {
              @Override
              public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
              }
            });

    filter.foreach(
        new VoidFunction<Integer>() {
          @Override
          public void call(Integer integer) throws Exception {
            System.out.println(integer);
          }
        });

    sparkContext.close();
  }

  /** flatMap案例：将文本行拆分为多个单词 */
  private static void flatMap() {

    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 构造集合
    List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

    // 并行化集合，创建RDD
    JavaRDD<String> lines = sparkContext.parallelize(lineList);

    // 对RDD执行flatMap算子，将每一行文本，拆分为多个单词
    // flatMap算子，在java中，接收的参数是FlatMapFunction
    // 我们需要自己定义FlatMapFunction的第二个泛型类型，即，代表了返回的新元素的类型
    // call()方法，返回的类型，不是U，而是Iterable<U>，这里的U也与第二个泛型类型相同
    // flatMap其实就是，接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回可以返回多个元素
    // 多个元素，即封装在Iterable集合中，可以使用ArrayList等集合
    // 新的RDD中，即封装了所有的新元素；也就是说，新的RDD的大小一定是 >= 原始RDD的大小
    JavaRDD<String> flatMapRdd =
        lines.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(StringUtils.split(s, " "));
              }
            });
    flatMapRdd.foreach(
        new VoidFunction<String>() {
          @Override
          public void call(String s) throws Exception {
            System.out.println(s);
          }
        });
    sparkContext.close();
  }

  /** groupByKey案例：按照班级对成绩进行分组 */
  private static void groupByKey() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // 模拟集合
    List<Tuple2<String, Integer>> scoreList =
        Arrays.asList(
            new Tuple2<>("class1", 80),
            new Tuple2<>("class2", 75),
            new Tuple2<>("class1", 90),
            new Tuple2<>("class2", 65));

    // 并行化集合，创建JavaPairRDD
    JavaPairRDD<String, Integer> scores = sparkContext.parallelizePairs(scoreList);

    // 针对scores RDD，执行groupByKey算子，对每个班级的成绩进行分组
    // groupByKey算子，返回的还是JavaPairRDD
    // 但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成Iterable这种集合类型
    // 也就是说，按照了key进行分组，那么每个key可能都会有多个value，此时多个value聚合成了Iterable
    // 那么接下来，我们是不是就可以通过groupedScores这种JavaPairRDD，很方便地处理某个分组内的数据
    JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

    groupedScores.foreach(
        new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
          @Override
          public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
            System.out.println("class: " + tuple2._1);
            for (Integer integer : tuple2._2) {
              System.out.println(integer);
            }
            System.out.println("==============================");
          }
        });
    sparkContext.close();
  }

  /** reduceByKey案例：统计每个班级的总分 */
  private static void reduceByKey() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 模拟集合
    List<Tuple2<String, Integer>> scoreList =
        Arrays.asList(
            new Tuple2<String, Integer>("class1", 80),
            new Tuple2<String, Integer>("class2", 75),
            new Tuple2<String, Integer>("class1", 90),
            new Tuple2<String, Integer>("class2", 65));

    // 并行化集合，创建JavaPairRDD
    JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);

    // 针对scores RDD，执行reduceByKey算子
    // 第一个泛型类型和第二个泛型类型，代表了原始RDD中的元素的value的类型
    // 因此对每个key进行reduce，都会依次将第一个、第二个value传入，将值再与第三个value传入
    // 因此此处，会自动定义两个泛型类型，代表call()方法的两个传入参数的类型
    // 第三个泛型类型，代表了每次reduce操作返回的值的类型，默认也是与原始RDD的value类型相同的
    // reduceByKey算法返回的RDD，还是JavaPairRDD<key, value>
    JavaPairRDD<String, Integer> pairRDD =
        scores.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
              }
            });

    pairRDD.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> tuple2) throws Exception {
            System.out.println(tuple2._1 + ": " + tuple2._2);
          }
        });
  }

  /** sortByKey案例：按照学生分数进行排序 */
  private static void sortByKey() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 模拟集合
    List<Tuple2<Integer, String>> scoreList =
        Arrays.asList(
            new Tuple2<Integer, String>(65, "leo"),
            new Tuple2<Integer, String>(50, "tom"),
            new Tuple2<Integer, String>(100, "marry"),
            new Tuple2<Integer, String>(80, "jack"));

    // 并行化集合，创建RDD
    JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);

    // 对scores RDD执行sortByKey算子
    // sortByKey其实就是根据key进行排序，可以手动指定升序，或者降序
    // 返回的，还是JavaPairRDD，其中的元素内容，都是和原始的RDD一模一样的
    // 但是就是RDD中的元素的顺序，不同了
    JavaPairRDD<Integer, String> sortedScores = scores.sortByKey();

    // 打印sortedScored RDD
    sortedScores.foreach(
        new VoidFunction<Tuple2<Integer, String>>() {
          @Override
          public void call(Tuple2<Integer, String> t) throws Exception {
            System.out.println(t._1 + ": " + t._2);
          }
        });

    // 关闭JavaSparkContext
    sc.close();
  }

  /** join案例：打印学生成绩 */
  private static void join() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 模拟集合
    List<Tuple2<Integer, String>> studentList =
        Arrays.asList(
            new Tuple2<Integer, String>(1, "leo"),
            new Tuple2<Integer, String>(2, "jack"),
            new Tuple2<Integer, String>(3, "tom"));

    List<Tuple2<Integer, Integer>> scoreList =
        Arrays.asList(
            new Tuple2<Integer, Integer>(1, 100),
            new Tuple2<Integer, Integer>(2, 90),
            new Tuple2<Integer, Integer>(3, 60));

    // 并行化两个RDD
    JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
    JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

    // 使用join算子关联两个RDD
    // join以后，还是会根据key进行join，并返回JavaPairRDD
    // 但是JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key的类型，因为是通过key进行join的
    // 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
    // join，就返回的RDD的每一个元素，就是通过key join上的一个pair
    // 什么意思呢？比如有(1, 1) (1, 2) (1, 3)的一个RDD
    // 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
    // join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))
    JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);

    studentScores.foreach(
        new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {

          @Override
          public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
            System.out.println("student id: " + t._1);
            System.out.println("student name: " + t._2._1);
            System.out.println("student score: " + t._2._2);
            System.out.println("===============================");
          }
        });

    // 关闭JavaSparkContext
    sc.close();
  }

  /** cogroup案例：打印学生成绩 */
  private static void cogroup() {
    // 创建SparkConf
    SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
    // 创建JavaSparkContext
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 模拟集合
    List<Tuple2<Integer, String>> studentList =
        Arrays.asList(
            new Tuple2<Integer, String>(1, "leo"),
            new Tuple2<Integer, String>(2, "jack"),
            new Tuple2<Integer, String>(3, "tom"));

    List<Tuple2<Integer, Integer>> scoreList =
        Arrays.asList(
            new Tuple2<Integer, Integer>(1, 100),
            new Tuple2<Integer, Integer>(2, 90),
            new Tuple2<Integer, Integer>(3, 60),
            new Tuple2<Integer, Integer>(1, 70),
            new Tuple2<Integer, Integer>(2, 80),
            new Tuple2<Integer, Integer>(3, 50));

    // 并行化两个RDD
    JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
    JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

    // cogroup与join不同
    // 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了
    // cogroup，不太好讲解，希望大家通过动手编写我们的案例，仔细体会其中的奥妙
    JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores =
        students.cogroup(scores);

    // 打印 studentScores RDD
    studentScores.foreach(
        new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {

          @Override
          public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
              throws Exception {
            System.out.println("student id: " + t._1);
            System.out.println("student name: " + t._2._1);
            System.out.println("student score: " + t._2._2);
            System.out.println("===============================");
          }
        });

    // 关闭JavaSparkContext
    sc.close();
  }
}
