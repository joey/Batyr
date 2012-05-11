package com.cloudera.batyr.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class LongWritableValues extends WritableValues<LongWritable> {

  private static Logger LOG = Logger.getLogger(LongWritableValues.class);

  public LongWritableValues(Iterable<LongWritable> iterable) {
    super(iterable);
  }
}
