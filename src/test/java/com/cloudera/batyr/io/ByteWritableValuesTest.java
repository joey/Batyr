package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.ByteWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ByteWritableValuesTest {

  @Test
  public void testIterator() {
    ByteWritableValues iterable = new ByteWritableValues(new Iterable<ByteWritable>() {

      @Override
      public Iterator<ByteWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
