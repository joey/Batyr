package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class IntWritableValuesTest {

  @Test
  public void testIterator() {
    IntWritableValues iterable = new IntWritableValues(new Iterable<IntWritable>() {

      @Override
      public Iterator<IntWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
