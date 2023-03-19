package com.losedream.spark.learn.core.sort;

import java.util.Objects;
import scala.Serializable;
import scala.math.Ordered;

/**
 * 自定义的二次排序key
 *
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

  private int first;

  private int second;

  public SecondarySortKey(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public static SecondarySortKey of(int first, int second) {
    return new SecondarySortKey(first, second);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecondarySortKey that = (SecondarySortKey) o;
    return first == that.first && second == that.second;
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  public int getFirst() {
    return first;
  }

  public void setFirst(int first) {
    this.first = first;
  }

  public int getSecond() {
    return second;
  }

  public void setSecond(int second) {
    this.second = second;
  }

  @Override
  public boolean $less(SecondarySortKey that) {
    if (this.first < that.first) {
      return true;
    } else {
      return second < that.second;
    }
  }

  @Override
  public boolean $greater(SecondarySortKey that) {
    if (first > that.first) {
      return true;
    } else {
      return second > that.second;
    }
  }

  @Override
  public boolean $less$eq(SecondarySortKey that) {
    if (first <= that.first) {
      return true;
    } else {
      return second <= that.second;
    }
  }

  @Override
  public boolean $greater$eq(SecondarySortKey that) {
    if (first >= that.first) {
      return true;
    } else {
      return second >= that.second;
    }
  }

  @Override
  public int compare(SecondarySortKey that) {
    if (this.first - that.first != 0) {
      return this.first - that.first;
    } else {
      return this.second - that.second;
    }
  }

  @Override
  public int compareTo(SecondarySortKey that) {
    if (this.first - that.getFirst() != 0) {
      return this.first - that.getFirst();
    } else {
      return this.second - that.getSecond();
    }
  }
}
