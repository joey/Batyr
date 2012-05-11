package com.cloudera.batyr.io;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

public class BooleanWritableValues extends WritableValues<BooleanWritable> {

  private static Logger LOG = Logger.getLogger(BooleanWritableValues.class);

  public BooleanWritableValues(Iterable<BooleanWritable> iterable) {
    super(iterable);
  }
}
