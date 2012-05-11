package com.cloudera.batyr.io;

import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;

public class FloatWritableValues extends WritableValues<FloatWritable> {

  private static Logger LOG = Logger.getLogger(FloatWritableValues.class);

  public FloatWritableValues(Iterable<FloatWritable> iterable) {
    super(iterable);
  }
}
