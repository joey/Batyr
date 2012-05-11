package com.cloudera.batyr.mapreduce.output;

import java.io.IOException;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class BatyrRecordWriter<K, V> extends RecordWriter<K, V> {

  private static Logger LOG = Logger.getLogger(BatyrRecordWriter.class);
  private OutputFormat<K, V> output;
  private TaskAttemptContext context;
  private RecordWriter<K, V> writer;

  public BatyrRecordWriter(OutputFormat<K, V> output, TaskAttemptContext context) {
    this.output = output;
    this.context = context;
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {
    if (writer == null) {
      context.getConfiguration().setClass("mapred.output.key.class", key.getClass(), Object.class);
      context.getConfiguration().setClass("mapred.output.value.class", value.getClass(), Object.class);
      writer = output.getRecordWriter(context);
    }
    writer.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (writer != null) {
      writer.close(context);
    }
  }
}
