package com.cloudera.batyr.types;

import static org.junit.Assert.*;
import org.junit.Test;

public class KeyWritableTest {

  @Test
  public void testCompareTo() {
    KeyWritable<String> other = new KeyWritable<String>("value");
    KeyWritable<String> instance = new KeyWritable<String>("value");
    int expResult = 0;
    int result = instance.compareTo(other);
    assertEquals(expResult, result);
  }

  @Test
  public void testHashCode() {
    KeyWritable<String> other = new KeyWritable<String>("value");
    KeyWritable<String> instance = new KeyWritable<String>("value");
    assertEquals(other.hashCode(), instance.hashCode());
  }

  @Test
  public void testEquals() {
    Object other = new KeyWritable<String>("value");
    KeyWritable<String> instance = new KeyWritable<String>("value");
    boolean expResult = true;
    boolean result = instance.equals(other);
    assertEquals(expResult, result);

    other = null;
    expResult = false;
    result = instance.equals(other);
    assertEquals(expResult, result);

    other = new ValueWritable<String>("value");
    expResult = false;
    result = instance.equals(other);
    assertEquals(expResult, result);

    other = new KeyWritable();
    expResult = false;
    result = instance.equals(other);
    assertEquals(expResult, result);
  }

}
