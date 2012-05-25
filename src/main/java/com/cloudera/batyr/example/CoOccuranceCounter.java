package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;

public class CoOccuranceCounter extends BatyrJob {

  public void map(long offset, String line) {
    String[] parts = line.split(",");
    String user = parts[0];
    write(user, 1l);
  }

  public void reduce(String user, Iterable<Long> counts) {
    long sum = 0;
    for (Long count : counts) {
      sum += count;
    }
    write(user, sum);
  }
}
