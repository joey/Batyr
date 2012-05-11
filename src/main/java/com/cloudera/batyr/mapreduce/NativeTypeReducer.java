package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.types.KeyWritable;
import com.cloudera.batyr.types.ValueWritable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class NativeTypeReducer<KEYOUT, VALUEOUT> extends Reducer<KeyWritable, ValueWritable, KEYOUT, VALUEOUT> {

  private static Logger LOG = Logger.getLogger(NativeTypeReducer.class);
  BatyrJob job;
  Method reduce;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getJobObject(context);
    job.setContext(context);
    for (Method method : job.getClass().getDeclaredMethods()) {
      if (method.getName().equals("reduce") && method.getParameterTypes().length == 2) {
        reduce = method;
        break;
      }
    }
  }

  @Override
  protected void reduce(KeyWritable key, Iterable<ValueWritable> values, Context context) throws IOException, InterruptedException {
    try {
      reduce.invoke(job, key.getValue(), new NativeIterable(values));
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }

  private static class NativeIterable implements Iterable<Object>, Iterator<Object> {

    private Iterator<ValueWritable> iterator;

    public NativeIterable(Iterable<ValueWritable> iterable) {
      this.iterator = iterable.iterator();
    }

    @Override
    public Iterator<Object> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Object next() {
      return iterator.next().getValue();
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }
}
