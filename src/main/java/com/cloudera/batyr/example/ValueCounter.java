package com.cloudera.batyr.example;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ValueCounter<KEY> extends Reducer<KEY, LongWritable, KEY, LongWritable> {

  private static Logger LOG = Logger.getLogger(ValueCounter.class);

  @Override
  protected void reduce(KEY key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long sum = 0l;
    for (LongWritable value : values) {
      sum += value.get();
    }
    context.write(key, new LongWritable(sum));
  }
}
