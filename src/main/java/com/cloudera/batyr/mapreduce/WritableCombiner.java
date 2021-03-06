package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.io.WritableValues;
import com.cloudera.batyr.reflect.MethodGrabber;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class WritableCombiner<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends Writable, VALUEOUT extends Writable> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private static Logger LOG = Logger.getLogger(WritableCombiner.class);
  BatyrJob job;
  Method combine;
  Class<? extends WritableValues> valuesType;
  Constructor<? extends WritableValues> valuesConstructor;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getDelegator(context).getJob();
    job.setContext(context);
    combine = MethodGrabber.getCombine(job);
    Class<?> clazz = combine.getParameterTypes()[1];
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
      combine.invoke(job, key, values);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }
}
