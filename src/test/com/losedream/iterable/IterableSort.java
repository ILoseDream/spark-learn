package com.losedream.iterable;

import com.google.common.collect.Lists;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author : zongri (｡￫‿￩｡)
 * @link : zhongri.ye@henhenchina.com
 * @since : 2023/3/19
 */
public class IterableSort {

  public static void main(String[] args) {
    Iterable<Integer> ints = Lists.newArrayList(75, 74, 39, 64, 98, 56);
    ints.forEach(System.out::println);
    System.err.println("===============================================");
    List<Integer> collect = StreamSupport.stream(ints.spliterator(), false).sorted(
            (o1, o2) -> o2 - o1)
        .collect(Collectors.toList());
    collect.forEach(System.out::println);
    System.err.println("===============================================");



  }

}
