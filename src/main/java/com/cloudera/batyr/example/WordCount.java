package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;
import java.io.IOException;

public class WordCount extends BatyrJob {

  public void map(long key, String value) throws IOException, InterruptedException {
    for (String word : value.split("[\\s\\p{Punct}]+")) {
      write(word, 1l);
    }
  }

  public void reduce(String key, Iterable<Long> values) throws IOException, InterruptedException {
    long sum = 0l;
    for (Long value : values) {
      sum += value;
    }
    write(key, sum);
  }
}