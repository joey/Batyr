package com.cloudera.batyr.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class NullInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return new ArrayList<InputSplit>(0);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static class NullRecordReader<K, V> extends RecordReader<K, V> {
    
    private K key;
    private V value;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      key = null;
      value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return false;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 1.0f;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
