package com.cloudera.batyr.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

public class IntWritableValues extends WritableValues<IntWritable> {

  private static Logger LOG = Logger.getLogger(IntWritableValues.class);

  public IntWritableValues(Iterable<IntWritable> iterable) {
    super(iterable);
  }
}
