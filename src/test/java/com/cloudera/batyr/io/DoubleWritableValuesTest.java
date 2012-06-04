package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class DoubleWritableValuesTest {

  @Test
  public void testIterator() {
    DoubleWritableValues iterable = new DoubleWritableValues(new Iterable<DoubleWritable>() {

      @Override
      public Iterator<DoubleWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
