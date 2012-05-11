package com.cloudera.batyr.types;

import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class KeyWritable<K extends Comparable<K>> extends ValueWritable<K> implements WritableComparable<KeyWritable<K>> {

  private static Logger LOG = Logger.getLogger(KeyWritable.class);

  public KeyWritable() {
  }

  public KeyWritable(K value) {
    super(value);
  }

  @Override
  public int compareTo(KeyWritable<K> o) {
    return getValue().compareTo(o.getValue());
  }
}
