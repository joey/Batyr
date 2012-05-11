package com.cloudera.batyr.io;

import org.apache.hadoop.io.ByteWritable;
import org.apache.log4j.Logger;

public class ByteWritableValues extends WritableValues<ByteWritable> {

  private static Logger LOG = Logger.getLogger(ByteWritableValues.class);

  public ByteWritableValues(Iterable<ByteWritable> iterable) {
    super(iterable);
  }
}
