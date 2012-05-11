package com.cloudera.batyr.example;

import com.cloudera.batyr.io.LongWritableValues;
import com.cloudera.batyr.mapreduce.BatyrJob;
import com.cloudera.batyr.mapreduce.FileInput;
import com.cloudera.batyr.mapreduce.FileOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import static com.cloudera.batyr.mapreduce.Format.*;

@FileInput(TextFile)
@FileOutput(SequenceFile)
public class SimpleWordCount extends BatyrJob {

  Pattern delimeter = Pattern.compile("[\\s\\p{Punct}]+");

  public void map(LongWritable key, Text value) throws IOException, InterruptedException {
    for (String word : delimeter.split(value.toString())) {
      write(new Text(word), new LongWritable(1l));
    }
  }

  public void reduce(Text key, LongWritableValues values) throws IOException, InterruptedException {
    long sum = 0l;
    for (LongWritable value : values) {
      sum += value.get();
    }
    write(key, new LongWritable(sum));
  }
}