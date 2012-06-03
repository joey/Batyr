package com.cloudera.batyr.reflect;

import com.cloudera.batyr.mapreduce.BatyrJob;

public class EmptyJob extends BatyrJob {

  public void map(String key, String value, String other) {
  }

}
