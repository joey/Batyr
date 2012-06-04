package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.VLongWritable;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class VLongWritableValuesTest {

  @Test
  public void testIterator() {
    VLongWritableValues iterable = new VLongWritableValues(new Iterable<VLongWritable>() {

      @Override
      public Iterator<VLongWritable> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
