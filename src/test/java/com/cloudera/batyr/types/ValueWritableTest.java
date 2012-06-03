package com.cloudera.batyr.types;

import java.io.*;
import java.util.*;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class ValueWritableTest {

  ByteArrayOutputStream outStream;
  DataInput in;
  DataOutput out;
  @Before
  public void setUp() {
    outStream = new ByteArrayOutputStream();
    out = new DataOutputStream(outStream);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBoolean() throws Exception {
    ValueWritable<Boolean> instance = new ValueWritable<Boolean>(Boolean.TRUE);
    assertEquals(true, instance.getBooleanValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testByte() throws Exception {
    ValueWritable<Byte> instance = new ValueWritable<Byte>((byte) 42);
    assertEquals((byte) 42, instance.getByteValue().byteValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testCharacter() throws Exception {
    ValueWritable<Character> instance = new ValueWritable<Character>('&');
    assertEquals('&', instance.getCharacterValue().charValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testShort() throws Exception {
    ValueWritable<Short> instance = new ValueWritable<Short>((short) 42);
    assertEquals((short) 42, instance.getShortValue().shortValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testInteger() throws Exception {
    ValueWritable<Integer> instance = new ValueWritable<Integer>(42);
    assertEquals(42, instance.getIntegerValue().intValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testLong() throws Exception {
    ValueWritable<Long> instance = new ValueWritable<Long>(42l);
    assertEquals(42l, instance.getLongValue().longValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testFloat() throws Exception {
    ValueWritable<Float> instance = new ValueWritable<Float>(42.0f);
    assertEquals(42.0f, instance.getFloatValue().floatValue(), 0.0000001);
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testDouble() throws Exception {
    ValueWritable<Double> instance = new ValueWritable<Double>(42.0);
    assertEquals(42.0, instance.getDoubleValue().doubleValue(), 0.0000001);
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testString() throws Exception {
    ValueWritable<String> instance = new ValueWritable<String>("string");
    assertEquals("string", instance.getStringValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testList() throws Exception {
    List list = new ArrayList();
    list.add(true);
    list.add(42);
    list.add("string");
    ValueWritable<List> instance = new ValueWritable<List>(list);
    assertEquals(list, instance.getListValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testSet() throws Exception {
    Set set = new HashSet();
    set.add("value1");
    set.add("value1");
    set.add("value2");
    set.add("value3");
    ValueWritable<Set> instance = new ValueWritable<Set>(set);
    assertEquals(set, instance.getSetValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testMap() throws Exception {
    Map map = new HashMap();
    map.put("value1", 0l);
    map.put("value1", 1l);
    map.put("value2", 2l);
    map.put("value3", 3l);
    ValueWritable<Map> instance = new ValueWritable<Map>(map);
    assertEquals(map, instance.getMapValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  public static class Pair implements Serializable {

    private static final long serialVersionUID = 5128526390440224064L;
    String first;
    String second;
    public Pair() {
    }

    public Pair(String first, String second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public String toString() {
      return "(" + first + "," + second + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Pair other = (Pair) obj;
      if ((this.first == null) ? (other.first != null) : !this.first.equals(other.first)) {
        return false;
      }
      if ((this.second == null) ? (other.second != null) : !this.second.equals(other.second)) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 97 * hash + (this.first != null ? this.first.hashCode() : 0);
      hash = 97 * hash + (this.second != null ? this.second.hashCode() : 0);
      return hash;
    }

  }
  @Test
  public void testSerializable() throws Exception {
    Pair pair = new Pair("one", "two");
    ValueWritable<Serializable> instance = new ValueWritable<Serializable>(pair);
    assertEquals(pair, instance.getSerializableValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test
  public void testNull() throws Exception {
    ValueWritable instance = new ValueWritable(null);
    assertEquals(null, instance.getSerializableValue());
    instance.write(out);
    in = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
    ValueWritable result = new ValueWritable();
    result.readFields(in);
    assertEquals(instance, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedType() throws Exception {
    ValueWritable<Object> instance = new ValueWritable<Object>(new Object());
  }

  @Test
  public void testToStringNullValue() throws Exception {
    ValueWritable instance = new ValueWritable(null);
    String expectedResult = "null";
    String result = instance.toString();
    assertEquals(expectedResult, result);
  }

  @Test
  public void testToStringValue() throws Exception {
    ValueWritable<String> instance = new ValueWritable<String>("value");
    String expectedResult = "value";
    String result = instance.toString();
    assertEquals(expectedResult, result);
  }

  @Test
  public void testEqualsNull() throws Exception {
    Object value = null;
    ValueWritable<String> instance = new ValueWritable<String>("value");
    boolean expectedResult = false;
    boolean result = instance.equals(value);
    assertEquals(expectedResult, result);
  }

  @Test
  public void testEqualsNonValueWritable() throws Exception {
    Object value = "value";
    ValueWritable<String> instance = new ValueWritable<String>("value");
    boolean expectedResult = false;
    boolean result = instance.equals(value);
    assertEquals(expectedResult, result);
  }

  @Test
  public void testEqualsNullValueWritable() throws Exception {
    Object value = new ValueWritable(null);
    ValueWritable<String> instance = new ValueWritable<String>("value");
    boolean expectedResult = false;
    boolean result = instance.equals(value);
    assertEquals(expectedResult, result);
  }

  @Test
  public void testEqualsDifferentTypes() throws Exception {
    Object value = new ValueWritable<Long>(42l);
    ValueWritable<String> instance = new ValueWritable<String>("value");
    boolean expectedResult = false;
    boolean result = instance.equals(value);
    assertEquals(expectedResult, result);
  }

  @Test
  public void testEqualsDifferentValues() throws Exception {
    Object value = new ValueWritable<String>("different value");
    ValueWritable<String> instance = new ValueWritable<String>("value");
    boolean expectedResult = false;
    boolean result = instance.equals(value);
    assertEquals(expectedResult, result);
  }

  @Test
  public void testHashCode() throws Exception {
    ValueWritable<String> value1 = new ValueWritable<String>("value");
    ValueWritable<String> value2 = new ValueWritable<String>("value");
    assertEquals(value1.hashCode(), value2.hashCode());

    value1 = new ValueWritable<String>("value1");
    value2 = new ValueWritable<String>("value2");
    assertNotSame(value1.hashCode(), value2.hashCode());

    value1.typeCode = null;
    value1.value = null;
    value2 = new ValueWritable<String>("value2");
    assertNotSame(value1.hashCode(), value2.hashCode());
  }

}
