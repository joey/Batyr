package com.cloudera.batyr.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

public class DoubleWritableValues extends WritableValues<DoubleWritable> {

  private static Logger LOG = Logger.getLogger(DoubleWritableValues.class);

  public DoubleWritableValues(Iterable<DoubleWritable> iterable) {
    super(iterable);
  }
}
