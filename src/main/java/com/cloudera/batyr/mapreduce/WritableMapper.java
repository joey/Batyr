package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.reflect.MethodGrabber;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class WritableMapper<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends Writable, VALUEOUT extends Writable> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private static Logger LOG = Logger.getLogger(WritableMapper.class);
  BatyrJob job;
  Method map;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    job = BatyrJob.getDelegator(context).getJob();
    job.setContext(context);
    map = MethodGrabber.getMap(job);
  }

  @Override
  protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
    try {
      map.invoke(job, key, value);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    } catch (IllegalArgumentException ex) {
      throw new IOException(ex);
    } catch (InvocationTargetException ex) {
      throw new IOException(ex);
    }
  }
}
