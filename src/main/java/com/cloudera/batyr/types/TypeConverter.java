package com.cloudera.batyr.types;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.log4j.Logger;

public class TypeConverter {

  private static Logger LOG = Logger.getLogger(TypeConverter.class);

  public static String toString(Object obj) {
    return obj.toString();
  }

  public static long toLong(Object obj) {
    if (obj instanceof Number) {
      return (long) ((Number) obj).longValue();
    } else if (obj instanceof CharSequence) {
      return (long) Long.valueOf(obj.toString());
    } else if (obj instanceof ByteWritable) {
      return (long) ((ByteWritable) obj).get();
    } else if (obj instanceof IntWritable) {
      return (long) ((IntWritable) obj).get();
    } else if (obj instanceof LongWritable) {
      return (long) ((LongWritable) obj).get();
    } else if (obj instanceof FloatWritable) {
      return (long) ((FloatWritable) obj).get();
    } else if (obj instanceof DoubleWritable) {
      return (long) ((DoubleWritable) obj).get();
    } else if (obj instanceof VIntWritable) {
      return (long) ((VIntWritable) obj).get();
    } else if (obj instanceof VLongWritable) {
      return (long) ((VLongWritable) obj).get();
    } else if (obj instanceof BooleanWritable) {
      return (long) (((BooleanWritable) obj).get() ? 1l : 0l);
    } else if (obj instanceof Text) {
      return (long) Long.valueOf(obj.toString());
    }
    throw new IllegalArgumentException("Can't convert " + obj.getClass() + " to a long");
  }

  public static boolean isConvertableToLong(Object obj) {
    if (obj instanceof Number
        || obj instanceof CharSequence
        || obj instanceof ByteWritable
        || obj instanceof IntWritable
        || obj instanceof LongWritable
        || obj instanceof FloatWritable
        || obj instanceof DoubleWritable
        || obj instanceof VIntWritable
        || obj instanceof VLongWritable
        || obj instanceof BooleanWritable
        || obj instanceof Text) {
      return true;
    }
    return false;
  }

  public static double toDouble(Object obj) {
    if (obj instanceof Number) {
      return (double) ((Number) obj).doubleValue();
    } else if (obj instanceof CharSequence) {
      return (double) Double.valueOf(obj.toString());
    } else if (obj instanceof ByteWritable) {
      return (double) ((ByteWritable) obj).get();
    } else if (obj instanceof IntWritable) {
      return (double) ((IntWritable) obj).get();
    } else if (obj instanceof LongWritable) {
      return (double) ((LongWritable) obj).get();
    } else if (obj instanceof FloatWritable) {
      return (double) ((FloatWritable) obj).get();
    } else if (obj instanceof DoubleWritable) {
      return (double) ((DoubleWritable) obj).get();
    } else if (obj instanceof VIntWritable) {
      return (double) ((VIntWritable) obj).get();
    } else if (obj instanceof VLongWritable) {
      return (double) ((VLongWritable) obj).get();
    } else if (obj instanceof BooleanWritable) {
      return (double) (((BooleanWritable) obj).get() ? 1.0 : 0.0);
    } else if (obj instanceof Text) {
      return (double) Double.valueOf(obj.toString());
    }
    throw new IllegalArgumentException("Can't convert " + obj.getClass() + " to a double");
  }

  public static boolean isConvertableToDouble(Object obj) {
    if (obj instanceof Number
        || obj instanceof CharSequence
        || obj instanceof ByteWritable
        || obj instanceof IntWritable
        || obj instanceof LongWritable
        || obj instanceof FloatWritable
        || obj instanceof DoubleWritable
        || obj instanceof VIntWritable
        || obj instanceof VLongWritable
        || obj instanceof BooleanWritable
        || obj instanceof Text) {
      return true;
    }
    return false;
  }
}
