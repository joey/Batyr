package com.cloudera.batyr.reflect;

import com.cloudera.batyr.mapreduce.BatyrJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TestJob extends BatyrJob {

  public void map(String key, String value) {
  }

  public void cleanup(Mapper.Context context) {
  }

  public void combine(String key, Iterable<String> values) {
  }

  public int getPartition(String key, String value, int numPartitions) {
    return 0;
  }

  public void reduce(String key, Iterable<String> values) {
  }

  private void cleanup(Reducer.Context context) {
  }

}
