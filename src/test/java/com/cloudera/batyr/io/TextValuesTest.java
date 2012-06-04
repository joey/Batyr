package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TextValuesTest {

  @Test
  public void testIterator() {
    TextValues iterable = new TextValues(new Iterable<Text>() {

      @Override
      public Iterator<Text> iterator() {
        return null;
      }

    });
    assertEquals(null, iterable.iterator());
  }

}
