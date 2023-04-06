package com.losedream.spark.learn.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * 简易版的连接池
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/6
 */
public class ConnectionPool {

  private static LinkedList<Connection> connectionQueue;

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static synchronized Connection getConnection() {
    try {
      if (connectionQueue == null) {
        connectionQueue = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
          Connection conn = DriverManager.getConnection(
              "jdbc:mysql://spark1:3306/testdb",
              "",
              "");
          connectionQueue.push(conn);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return connectionQueue.poll();
  }

  /**
   * 还回去一个连接
   */
  public static void returnConnection(Connection conn) {
    connectionQueue.push(conn);
  }

}
