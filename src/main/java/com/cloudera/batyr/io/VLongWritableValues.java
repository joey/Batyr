package com.cloudera.batyr.io;

import org.apache.hadoop.io.VLongWritable;
import org.apache.log4j.Logger;

public class VLongWritableValues extends WritableValues<VLongWritable> {

  private static Logger LOG = Logger.getLogger(VLongWritableValues.class);

  public VLongWritableValues(Iterable<VLongWritable> iterable) {
    super(iterable);
  }
}
