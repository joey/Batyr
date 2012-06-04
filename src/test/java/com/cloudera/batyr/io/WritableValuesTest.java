package com.cloudera.batyr.io;

import java.util.Iterator;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class WritableValuesTest {

  @Test
  public void testSetIterable() {
    Iterable<Text> iterable = new Iterable<Text>() {

      @Override
      public Iterator<Text> iterator() {
        return null;
      }

    };
    WritableValues<Text> instance = new WritableValues<Text>(iterable);
    instance.setIterable(iterable);
  }

  @Test
  public void testIterator() {
    Iterable<Text> iterable = new Iterable<Text>() {

      @Override
      public Iterator<Text> iterator() {
        return null;
      }

    };
    WritableValues<Text> instance = new WritableValues<Text>(iterable);
    Iterator expResult = null;
    Iterator result = instance.iterator();
    assertEquals(expResult, result);
  }

}
