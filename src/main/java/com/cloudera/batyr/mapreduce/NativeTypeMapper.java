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
  Class<?> keyClass;
  Class<?> valueClass;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getDelegator(context).getJob();
    job.setContext(context);
    for (Method method : job.getClass().getDeclaredMethods()) {
      if (method.getName().equals("map") && method.getParameterTypes().length == 2) {
        map = method;
        keyClass = map.getParameterTypes()[0];
        valueClass = map.getParameterTypes()[1];
        break;
      }
    }
  }

  @Override
  protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
    try {
      Object k = null;
      Object v = null;
      if (keyClass == long.class) {
        k = TypeConverter.toLong(key);
      } else if (keyClass == String.class) {
        k = TypeConverter.toString(key);
      }
      
      if (valueClass == long.class) {
        v = TypeConverter.toLong(value);
      } else if (valueClass == String.class) {
        v = TypeConverter.toString(value);
      }
      
      map.invoke(job, k, v);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }

}
