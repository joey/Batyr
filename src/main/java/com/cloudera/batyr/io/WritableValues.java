package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class WritableValues<V extends Writable> implements Iterable<V> {

  private static Logger LOG = Logger.getLogger(WritableValues.class);
  private Iterable<V> iterable;

  public WritableValues(Iterable<V> iterable) {
    this.iterable = iterable;
  }

  public void setIterable(Iterable<V> iterable) {
    this.iterable = iterable;
  }

  @Override
  public Iterator<V> iterator() {
    return iterable.iterator();
  }
}
