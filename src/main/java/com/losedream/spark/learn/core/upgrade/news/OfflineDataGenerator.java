package com.losedream.spark.learn.core.upgrade.news;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 离线数据生成器
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/17
 */
public class OfflineDataGenerator {

  public static void main(String[] args) {
    StringBuilder builder = new StringBuilder(StringUtils.EMPTY);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    String[] sections = new String[]{"country", "international", "sport", "entertainment", "movie",
        "carton", "tv-show", "technology", "internet", "car"};
    int[] newOldUserArr = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.add(Calendar.DAY_OF_YEAR, -1);
    Date yesterday = calendar.getTime();
    String date = sdf.format(yesterday);
    for (int i = 0; i < 100; i++) {
      // 生成时间戳
      long timestamp = new Date().getTime();

      // 生成随机userid（默认1000注册用户，每天1/10的访客是未注册用户）
      Long userId = 0L;
      int newOldUser = newOldUserArr[RandomUtils.nextInt(0, 10)];
      if (newOldUser == 1) {
        userId = null;
      } else {
        userId = (long) RandomUtils.nextInt(0, 1000);
      }

      // 生成随机pageid（总共1k个页面）
      Long pageid = (long) RandomUtils.nextInt(0, 1000);

      // 生成随机版块（总共10个版块）
      String section = sections[RandomUtils.nextInt(0, 10)];

      // 生成固定的行为，view
      String action = "view";

      builder.append(date).append("")
          .append(timestamp).append("")
          .append(userId).append("")
          .append(pageid).append("")
          .append(section).append("")
          .append(action).append("\n");
    }

    // 生成10条注册数据
    for (int i = 0; i < 10; i++) {
      // 生成时间戳
      long timestamp = new Date().getTime();

      // 新用户都是userid为null
      Long userid = null;

      // 生成随机pageid，都是null
      Long pageid = null;

      // 生成随机版块，都是null
      String section = null;

      // 生成固定的行为，view
      String action = "register";

      builder.append(date).append("")
          .append(timestamp).append("")
          .append(userid).append("")
          .append(pageid).append("")
          .append(section).append("")
          .append(action).append("\n");
    }

    try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
        Files.newOutputStream(Paths.get("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/logs/access.log"))))) {
      pw.write(builder.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
