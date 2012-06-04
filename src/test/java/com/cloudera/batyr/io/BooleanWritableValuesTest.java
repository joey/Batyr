package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.BooleanWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BooleanWritableValuesTest {

  @Test
  public void testIterator() {
    BooleanWritableValues iterable = new BooleanWritableValues(new Iterable<BooleanWritable>() {

      @Override
      public Iterator<BooleanWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
