package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class LongWritableValuesTest {

  @Test
  public void testIterator() {
    LongWritableValues iterable = new LongWritableValues(new Iterable<LongWritable>() {

      @Override
      public Iterator<LongWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
