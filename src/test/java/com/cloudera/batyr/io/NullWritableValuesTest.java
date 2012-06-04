package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class NullWritableValuesTest {

  @Test
  public void testIterator() {
    NullWritableValues iterable = new NullWritableValues(new Iterable<NullWritable>() {

      @Override
      public Iterator<NullWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
