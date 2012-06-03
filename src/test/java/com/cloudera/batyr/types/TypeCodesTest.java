package com.cloudera.batyr.types;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TypeCodesTest {

  @Test
  public void testGet() {
    Class<?> key = Object.class;
    TypeCodes instance = TypeCodes.get();
    TypeCode expResult = null;
    TypeCode result = instance.get(key);
    assertEquals(expResult, result);
  }

}
