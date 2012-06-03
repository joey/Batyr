package com.cloudera.batyr.types;

import org.apache.hadoop.io.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class TypeConverterTest {

  @Test
  public void testToString() {
    System.out.println("toString");
    Object obj = 42l;
    String expResult = "42";
    String result = TypeConverter.toString(obj);
    assertEquals(expResult, result);
  }

  @Test
  public void testToLong() {
    Object obj;
    long expResult;
    long result;

    obj = 42.0;
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = "42";
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new ByteWritable((byte) 42);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new IntWritable(42);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new LongWritable(42L);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new FloatWritable(42.0f);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new DoubleWritable(42.0);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new VIntWritable(42);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new VLongWritable(42);
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new BooleanWritable(true);
    expResult = 1L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new BooleanWritable(false);
    expResult = 0L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);

    obj = new Text("42");
    expResult = 42L;
    result = TypeConverter.toLong(obj);
    assertEquals(expResult, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToLongIllegalArgument() {
    TypeConverter.toLong(new Object());
  }

  @Test
  public void testIsConvertableToLong() {
    Object obj;
    boolean expResult;
    boolean result;

    obj = 42.0;
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = "42";
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new ByteWritable((byte) 42);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new IntWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new LongWritable(42L);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new FloatWritable(42.0f);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new DoubleWritable(42.0);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new VIntWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new VLongWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new BooleanWritable(true);
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new Text("42");
    expResult = true;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);

    obj = new Object();
    expResult = false;
    result = TypeConverter.isConvertableToLong(obj);
    assertEquals(expResult, result);
  }

  @Test
  public void testToDouble() {
    Object obj;
    double expResult;
    double result;

    obj = 42L;
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = "42.0";
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new ByteWritable((byte) 42);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new IntWritable(42);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new LongWritable(42L);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new FloatWritable(42.0f);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new DoubleWritable(42.0);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new VIntWritable(42);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new VLongWritable(42);
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new BooleanWritable(true);
    expResult = 1.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new BooleanWritable(false);
    expResult = 0.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);

    obj = new Text("42.0");
    expResult = 42.0;
    result = TypeConverter.toDouble(obj);
    assertEquals(expResult, result, 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToDoubleIllegalArgument() {
    TypeConverter.toDouble(new Object());
  }

  @Test
  public void testIsConvertableToDouble() {
    Object obj;
    boolean expResult;
    boolean result;

    obj = 42L;
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = "42.0";
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new ByteWritable((byte) 42);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new IntWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new LongWritable(42L);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new FloatWritable(42.0f);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new DoubleWritable(42.0);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new VIntWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new VLongWritable(42);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new BooleanWritable(true);
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new Text("42.0");
    expResult = true;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);

    obj = new Object();
    expResult = false;
    result = TypeConverter.isConvertableToDouble(obj);
    assertEquals(expResult, result);
  }

}
