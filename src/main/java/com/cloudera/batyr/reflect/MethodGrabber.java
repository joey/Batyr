package com.cloudera.batyr.reflect;

import com.cloudera.batyr.mapreduce.BatyrJob;
import java.lang.reflect.Method;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MethodGrabber {

  private static Logger LOG = Logger.getLogger(MethodGrabber.class);

  public static Method getMethod(Object obj, String name, int numParameters) {
    for (Method method : obj.getClass().getDeclaredMethods()) {
      if (method.getName().equals(name) && method.getParameterTypes().length == numParameters) {
        method.setAccessible(true);
        return method;
      }
    }
    return null;
  }

  public static Method getMap(BatyrJob job) {
    return getMethod(job, "map", 2);
  }

  public static Method getGetPartition(BatyrJob job) {
    return getMethod(job, "getPartition", 3);
  }

  public static Method getReduce(BatyrJob job) {
    return getMethod(job, "reduce", 2);
  }

  public static Method getCombine(BatyrJob job) {
    return getMethod(job, "combine", 2);
  }

  public static Method getReduceCleanup(BatyrJob job) {
    try {
      Method cleanup = job.getClass().getDeclaredMethod("cleanup", Reducer.Context.class);
      cleanup.setAccessible(true);
      return cleanup;
    } catch (NoSuchMethodException ex) {
      LOG.info("No reduce cleanup method");
      return null;
    } catch (SecurityException ex) {
      LOG.error("Can't get reduce cleanup method", ex);
      return null;
    }
  }
}
