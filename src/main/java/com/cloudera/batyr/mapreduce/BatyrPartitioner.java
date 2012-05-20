package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.reflect.MethodGrabber;
import com.cloudera.batyr.types.KeyWritable;
import com.cloudera.batyr.types.ValueWritable;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class BatyrPartitioner extends Partitioner<KeyWritable, ValueWritable> implements Configurable {

  private static Logger LOG = Logger.getLogger(BatyrPartitioner.class);
  Configuration conf;
  BatyrJob job;
  Method getPartition;
  Class<?> keyClass;
  Class<?> valueClass;

  @Override
  public int getPartition(KeyWritable key, ValueWritable value, int numPartitions) {
    try {
      LOG.error(key.getValue().getClass());
      LOG.error(value.getValue().getClass());
      return (Integer) getPartition.invoke(job, key.getValue(), value.getValue(), numPartitions);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    job = BatyrJob.getDelegator(conf).getJob();
    getPartition = MethodGrabber.getGetPartition(job);
    keyClass = getPartition.getParameterTypes()[0];
    valueClass = getPartition.getParameterTypes()[1];
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
