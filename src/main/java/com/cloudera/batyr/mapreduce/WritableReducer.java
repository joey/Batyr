package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.io.WritableValues;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class WritableReducer<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends Writable, VALUEOUT extends Writable> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private static Logger LOG = Logger.getLogger(WritableReducer.class);
  BatyrJob job;
  Method reduce;
  Class<? extends WritableValues> valuesType;
  Constructor<? extends WritableValues> valuesConstructor;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getJobObject(context);
    job.setContext(context);
    for (Method method : job.getClass().getDeclaredMethods()) {
      if (method.getName().equals("reduce") && method.getParameterTypes().length == 2) {
        reduce = method;
        Class<?> clazz = reduce.getParameterTypes()[1];
        if (WritableValues.class.isAssignableFrom(clazz)) {
          valuesType = clazz.asSubclass(WritableValues.class);
          try {
            valuesConstructor = valuesType.getConstructor(Iterable.class);
          } catch (NoSuchMethodException ex) {
            valuesConstructor = null;
            LOG.warn("No constructor that takes an Iterable, assuming a generic Iterable will be passed", ex);
          } catch (SecurityException ex) {
            valuesConstructor = null;
            LOG.warn("No public constructor that takes an Iterable, assuming a generic Iterable will be passed", ex);
          }
        }
        break;
      }
    }
  }

  @Override
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
    try {
      if (valuesConstructor != null) {
        try {
          values = valuesConstructor.newInstance(values);
        } catch (InstantiationException ex) {
          LOG.error("Can't instantiate the " + valuesType.getSimpleName() + " object, passing a naked Iterable to reduce()", ex);
        }
      }
      reduce.invoke(job, key, values);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }
}
