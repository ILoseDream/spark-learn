package com.losedream.spark.learn.core.upgrade.applog;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class DataGenerator {

  public static void main(String[] args) {
    Random random = new Random();

    // 生成100个deviceID
    List<String> deviceIDs = IntStream.range(0, 100)
        .mapToObj(value -> DataGenerator.getRandomUUID())
        .collect(Collectors.toList());

    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      // 生成随机时间戳
      Calendar cal = Calendar.getInstance();
      cal.setTime(new Date());
      cal.add(Calendar.MINUTE, -random.nextInt(600));
      long timestamp = cal.getTime().getTime();

      // 生成随机deviceID
      String deviceID = deviceIDs.get(random.nextInt(100));

      // 生成随机的上行流量
      long upTraffic = random.nextInt(100000);
      // 生成随机的下行流量
      long downTraffic = random.nextInt(100000);

      buffer.append(timestamp).append("\t")
          .append(deviceID).append("\t")
          .append(upTraffic).append("\t")
          .append(downTraffic).append("\n");
    }

    try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
        Files.newOutputStream(Paths.get("/Users/losedream/learn/java_code/spark/spark-learn/spark-learn/logs/access.log"))))) {
      pw.write(buffer.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static String getRandomUUID() {
    return UUID.randomUUID().toString().replace("-", "");
  }

}
