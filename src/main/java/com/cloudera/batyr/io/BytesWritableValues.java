package com.cloudera.batyr.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

public class BytesWritableValues extends WritableValues<BytesWritable> {

  private static Logger LOG = Logger.getLogger(BytesWritableValues.class);

  public BytesWritableValues(Iterable<BytesWritable> iterable) {
    super(iterable);
  }
}
