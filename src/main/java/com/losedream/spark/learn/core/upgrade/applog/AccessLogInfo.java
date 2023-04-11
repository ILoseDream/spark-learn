package com.losedream.spark.learn.core.upgrade.applog;

import java.awt.peer.LabelPeer;
import java.io.Serializable;

/**
 * 访问日志信息类（可序列化）
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class AccessLogInfo implements Serializable {

  /**
   * 时间戳
   */
  private long timestamp;

  /**
   * 上行流量
   */
  private long upTraffic;

  /**
   * 下行流量
   */
  private long downTraffic;

  public AccessLogInfo() {
  }

  public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
    this.timestamp = timestamp;
    this.upTraffic = upTraffic;
    this.downTraffic = downTraffic;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getUpTraffic() {
    return upTraffic;
  }

  public void setUpTraffic(long upTraffic) {
    this.upTraffic = upTraffic;
  }

  public long getDownTraffic() {
    return downTraffic;
  }

  public void setDownTraffic(long downTraffic) {
    this.downTraffic = downTraffic;
  }
}
