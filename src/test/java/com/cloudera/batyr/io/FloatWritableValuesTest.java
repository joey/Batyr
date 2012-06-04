package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class FloatWritableValuesTest {

  @Test
  public void testIterator() {
    FloatWritableValues iterable = new FloatWritableValues(new Iterable<FloatWritable>() {

      @Override
      public Iterator<FloatWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
