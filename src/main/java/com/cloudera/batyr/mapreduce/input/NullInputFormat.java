package com.cloudera.batyr.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.*;

public class NullInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return new ArrayList<InputSplit>(0);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new NullRecordReader<K, V>();
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
