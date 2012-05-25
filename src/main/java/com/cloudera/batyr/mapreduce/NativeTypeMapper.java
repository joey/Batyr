package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.reflect.MethodGrabber;
import com.cloudera.batyr.types.KeyWritable;
import com.cloudera.batyr.types.TypeConverter;
import com.cloudera.batyr.types.ValueWritable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class NativeTypeMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, KeyWritable, ValueWritable> {

  private static Logger LOG = Logger.getLogger(NativeTypeMapper.class);
  BatyrJob job;
  Method map;
  Method combine;
  Class<?> keyClass;
  Class<?> valueClass;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getDelegator(context).getJob();
    job.setContext(context);
    map = MethodGrabber.getMap(job);
    keyClass = map.getParameterTypes()[0];
    valueClass = map.getParameterTypes()[1];

    combine = MethodGrabber.getCombine(job);
    if (combine != null) {
      job.setInMemoryCombiner(combine);
    }
  }

  @Override
  protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
    Object k = null;
    Object v = null;
    try {
      if (key instanceof KeyWritable) {
        key = (KEYIN) ((KeyWritable) key).getValue();
      }

      if (keyClass == long.class) {
        LOG.debug("Converting key to long");
        k = TypeConverter.toLong(key);
      } else if (keyClass == String.class) {
        LOG.debug("Converting key to String");
        k = TypeConverter.toString(key);
      } else if (keyClass == double.class) {
        LOG.debug("Converting key to double");
        k = TypeConverter.toDouble(key);
      }

      if (value instanceof ValueWritable) {
        value = (VALUEIN) ((ValueWritable) value).getValue();
      }

      if (valueClass == long.class) {
        LOG.debug("Converting value to long");
        v = TypeConverter.toLong(value);
      } else if (valueClass == String.class) {
        LOG.debug("Converting value to String");
        v = TypeConverter.toString(value);
      } else if (valueClass == double.class) {
        LOG.debug("Converting value to double");
        v = TypeConverter.toDouble(value);
      }

      map.invoke(job, k, v);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      LOG.error(String.format("Illegal arguments, called map(%s,%s), expected map(%s,%s)",
          k == null ? "null" : k.getClass().getName(), v == null ? "null" : v.getClass().getName(),
          keyClass.getName(), valueClass.getName()), ex);
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (combine != null) {
      job.runInMemoryCombiner();
    }
  }
}
