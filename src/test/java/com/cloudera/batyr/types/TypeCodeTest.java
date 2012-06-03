package com.cloudera.batyr.types;

import org.junit.Test;

public class TypeCodeTest {

  @Test(expected = IllegalArgumentException.class)
  public void testFromCode() {
    int code = -42;
    TypeCode result = TypeCode.fromCode(code);
  }

}
