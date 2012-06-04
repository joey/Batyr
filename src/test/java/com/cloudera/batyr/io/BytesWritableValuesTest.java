package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BytesWritableValuesTest {

  @Test
  public void testIterator() {
    BytesWritableValues iterable = new BytesWritableValues(new Iterable<BytesWritable>() {

      @Override
      public Iterator<BytesWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
