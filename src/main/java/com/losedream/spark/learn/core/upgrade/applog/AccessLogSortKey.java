package com.losedream.spark.learn.core.upgrade.applog;

import java.io.Serializable;
import java.util.Objects;
import scala.math.Ordered;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/4/11
 */
public class AccessLogSortKey implements Ordered<AccessLogSortKey>, Serializable {

  private long upTraffic;
  private long downTraffic;
  private long timestamp;

  public AccessLogSortKey() {
  }

  public AccessLogSortKey(long upTraffic, long downTraffic, long timestamp) {
    this.upTraffic = upTraffic;
    this.downTraffic = downTraffic;
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

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccessLogSortKey that = (AccessLogSortKey) o;
    return upTraffic == that.upTraffic && downTraffic == that.downTraffic
        && timestamp == that.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(upTraffic, downTraffic, timestamp);
  }

  @Override
  public boolean $greater(AccessLogSortKey other) {
    if (upTraffic > other.upTraffic) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic > other.downTraffic) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic == other.downTraffic &&
        timestamp > other.timestamp) {
      return true;
    }
    return false;
  }

  @Override
  public boolean $greater$eq(AccessLogSortKey other) {
    if ($greater(other)) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic == other.downTraffic &&
        timestamp == other.timestamp) {
      return true;
    }
    return false;
  }

  @Override
  public boolean $less(AccessLogSortKey other) {
    if (upTraffic < other.upTraffic) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic < other.downTraffic) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic == other.downTraffic &&
        timestamp < other.timestamp) {
      return true;
    }
    return false;
  }

  @Override
  public boolean $less$eq(AccessLogSortKey other) {
    if ($less(other)) {
      return true;
    } else if (upTraffic == other.upTraffic &&
        downTraffic == other.downTraffic &&
        timestamp == other.timestamp) {
      return true;
    }
    return false;
  }

  @Override
  public int compare(AccessLogSortKey other) {
    if (upTraffic - other.upTraffic != 0) {
      return (int) (upTraffic - other.upTraffic);
    } else if (downTraffic - other.downTraffic != 0) {
      return (int) (downTraffic - other.downTraffic);
    } else if (timestamp - other.timestamp != 0) {
      return (int) (timestamp - other.timestamp);
    }
    return 0;
  }

  @Override
  public int compareTo(AccessLogSortKey other) {
    if (upTraffic - other.upTraffic != 0) {
      return (int) (upTraffic - other.upTraffic);
    } else if (downTraffic - other.downTraffic != 0) {
      return (int) (downTraffic - other.downTraffic);
    } else if (timestamp - other.timestamp != 0) {
      return (int) (timestamp - other.timestamp);
    }
    return 0;
  }

  @Override
  public String toString() {
    return "AccessLogSortKey{" +
        "upTraffic=" + upTraffic +
        ", downTraffic=" + downTraffic +
        ", timestamp=" + timestamp +
        '}';
  }
}
