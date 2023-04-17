package com.losedream.spark.learn.core.upgrade.news;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 新闻网站关键指标离线统计Spark作业
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/17
 */
public class NewsOfflineStatSpark {

  public static void main(String[] args) {
    // 一般来说，在小公司中，可能就是将我们的spark作业使用linux的crontab进行调度
    // 将作业jar放在一台安装了spark客户端的机器上，并编写了对应的spark-submit shell脚本
    // 在crontab中可以配置，比如说每天凌晨3点执行一次spark-submit shell脚本，提交一次spark作业
    // 一般来说，离线的spark作业，每次运行，都是去计算昨天的数据

    // 大公司总，可能是使用较为复杂的开源大数据作业调度平台，比如常用的有azkaban、oozie等
    // 但是，最大的那几个互联网公司，比如说BAT、美团、京东，作业调度平台，都是自己开发的
    // 我们就会将开发好的Spark作业，以及对应的spark-submit shell脚本，配置在调度平台上，几点运行
    // 同理，每次运行，都是计算昨天的数据

    // 一般来说，每次spark作业计算出来的结果，实际上，大部分情况下，都会写入mysql等存储
    // 这样的话，我们可以基于mysql，用java web技术开发一套系统平台，来使用图表的方式展示每次spark计算
    // 出来的关键指标
    // 比如用折线图，可以反映最近一周的每天的用户跳出率的变化

    // 也可以通过页面，给用户提供一个查询表单，可以查询指定的页面的最近一周的pv变化
    // date pageId pv
    // 插入mysql中，后面用户就可以查询指定日期段内的某个page对应的所有pv，然后用折线图来反映变化曲线

    // 拿到昨天的日期，去hive表中，针对昨天的数据执行SQL语句
    String yesterday = getYesterday();

    // 创建SparkConf以及Spark上下文
    SparkConf sparkConf = new SparkConf()
        .setAppName("NewsOfflineStatSpark")
        .setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    HiveContext hiveContext = new HiveContext(sparkContext.sc());

    // 开发第一个关键指标：页面pv统计以及排序
    calculateDailyPagePv(hiveContext, yesterday);

    // 开发第二个关键指标：页面uv统计以及排序
    calculateDailyPageUv(hiveContext, yesterday);

    // 开发第三个关键指标：新用户注册比率统计
    calculateDailyNewUserRegisterRate(hiveContext, yesterday);

    // 开发第四个关键指标：用户跳出率统计
    calculateDailyUserJumpRate(hiveContext, yesterday);

    // 开发第五个关键指标：版块热度排行榜
    calculateDailySectionPvSort(hiveContext, yesterday);

    sparkContext.close();
  }

  private static void calculateDailySectionPvSort(HiveContext hiveContext, String yesterday) {
    String sql =
        "SELECT "
            + "date,"
            + "section,"
            + "pv "
            + "FROM ( "
            + "SELECT "
            + "date,"
            + "section,"
            + "count(*) pv "
            + "FROM news_access "
            + "WHERE action='view' "
            + "AND date='" + yesterday + "' "
            + "GROUP BY date,section "
            + ") t "
            + "ORDER BY pv DESC ";
    DataFrame df = hiveContext.sql(sql);
    df.show();
  }

  private static void calculateDailyUserJumpRate(HiveContext hiveContext, String yesterday) {
    // 计算已注册用户的昨天的总的访问pv
    String sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + yesterday
        + "' AND userid IS NOT NULL ";

    // 已注册用户的昨天跳出的总数
    String sql2 =
        "SELECT count(*) FROM ( SELECT count(*) cnt FROM news_access WHERE action='view' AND date='"
            + yesterday + "' AND userid IS NOT NULL GROUP BY userid HAVING cnt=1 ) t ";

    // 执行两条SQL，获取结果
    Object result1 = hiveContext.sql(sql1).collect()[0].get(0);
    long number1 = 0L;
    if (result1 != null) {
      number1 = Long.parseLong(String.valueOf(result1));
    }

    Object result2 = hiveContext.sql(sql2).collect()[0].get(0);
    long number2 = 0L;
    if (result2 != null) {
      number2 = Long.parseLong(String.valueOf(result2));
    }

    // 计算结果
    System.out.println("======================" + number1 + "======================");
    System.out.println("======================" + number2 + "======================");
    double rate = (double) number2 / (double) number1;
    System.out.println("======================" + formatDouble(rate) + "======================");
  }

  private static void calculateDailyNewUserRegisterRate(HiveContext hiveContext, String yesterday) {
    // 昨天所有访问行为中，userid为null，新用户的访问总数
    String sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + yesterday
        + "' AND userid IS NULL";
    // 昨天的总注册用户数
    String sql2 =
        "SELECT count(*) FROM news_access WHERE action='register' AND date='" + yesterday + "' ";

    // 执行两条SQL，获取结果
    Object result1 = hiveContext.sql(sql1).collect()[0].get(0);
    long number1 = 0L;
    if (result1 != null) {
      number1 = Long.parseLong(String.valueOf(result1));
    }

    Object result2 = hiveContext.sql(sql2).collect()[0].get(0);
    long number2 = 0L;
    if (result2 != null) {
      number2 = Long.parseLong(String.valueOf(result2));
    }

    // 计算结果
    System.out.println("======================" + number1 + "======================");
    System.out.println("======================" + number2 + "======================");
    double rate = (double) number2 / (double) number1;
    System.out.println("======================" + formatDouble(rate) + "======================");
  }

  /**
   * 格式化小数
   *
   * @param num num
   * @return 格式化小数
   */
  private static double formatDouble(double num) {
    BigDecimal bd = new BigDecimal(num);
    return bd.setScale(2, RoundingMode.HALF_UP).doubleValue();
  }

  private static void calculateDailyPageUv(HiveContext hiveContext, String yesterday) {
    String sql =
        "SELECT "
            + "date,"
            + "pageid,"
            + "uv "
            + "FROM ( "
            + "SELECT "
            + "date,"
            + "pageid,"
            + "count(*) uv "
            + "FROM ( "
            + "SELECT "
            + "date,"
            + "pageid,"
            + "userid "
            + "FROM news_access "
            + "WHERE action='view' "
            + "AND date='" + yesterday + "' "
            + "GROUP BY date,pageid,userid "
            + ") t2 "
            + "GROUP BY date,pageid "
            + ") t "
            + "ORDER BY uv DESC ";
    DataFrame df = hiveContext.sql(sql);

    df.show();
  }

  /**
   * 计算每天每个页面的pv以及排序 排序的好处：排序后，插入mysql，java web系统要查询每天pv top10的页面，直接查询mysql表limit 10就可以
   * 如果我们这里不排序，那么java web系统就要做排序，反而会影响java web系统的性能，以及用户响应时间
   *
   * @param hiveContext hiveContext
   * @param yesterday   yesterday
   */
  private static void calculateDailyPagePv(HiveContext hiveContext, String yesterday) {
    String sql =
        "SELECT "
            + "date,"
            + "pageid,"
            + "pv "
            + "FROM ( "
            + "SELECT "
            + "date,"
            + "pageid,"
            + "count(*) pv "
            + "FROM news_access "
            + "WHERE action='view' "
            + "AND date='" + yesterday + "' "
            + "GROUP BY date,pageid "
            + ") t "
            + "ORDER BY pv DESC ";
    DataFrame dataFrame = hiveContext.sql(sql);
    // 在这里，我们也可以转换成一个RDD，然后对RDD执行一个foreach算子
    // 在foreach算子中，将数据写入mysql中
    dataFrame.show();
  }

  private static String getYesterday() {
    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_YEAR, -1);

    Date yesterday = cal.getTime();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    return sdf.format(yesterday);
  }

}
