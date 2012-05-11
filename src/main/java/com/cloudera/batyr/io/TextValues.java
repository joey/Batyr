package com.cloudera.batyr.io;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TextValues extends WritableValues<Text> {

  private static Logger LOG = Logger.getLogger(TextValues.class);

  public TextValues(Iterable<Text> iterable) {
    super(iterable);
  }
}
