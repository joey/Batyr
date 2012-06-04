package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.VIntWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class VIntWritableValuesTest {

  @Test
  public void testIterator() {
    VIntWritableValues iterable = new VIntWritableValues(new Iterable<VIntWritable>() {

      @Override
      public Iterator<VIntWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
