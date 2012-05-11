package com.cloudera.batyr.io;

import org.apache.hadoop.io.VIntWritable;
import org.apache.log4j.Logger;

public class VIntWritableValues extends WritableValues<VIntWritable> {

  private static Logger LOG = Logger.getLogger(VIntWritableValues.class);

  public VIntWritableValues(Iterable<VIntWritable> iterable) {
    super(iterable);
  }
}
