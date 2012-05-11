package com.cloudera.batyr.io;

import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class NullWritableValues extends WritableValues<NullWritable> {

  private static Logger LOG = Logger.getLogger(NullWritableValues.class);

  public NullWritableValues(Iterable<NullWritable> iterable) {
    super(iterable);
  }
}
