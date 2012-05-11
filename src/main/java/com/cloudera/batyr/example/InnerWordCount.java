package com.cloudera.batyr.example;

import java.util.regex.Pattern;
import com.cloudera.batyr.mapreduce.BatyrJob;
import com.cloudera.batyr.mapreduce.FileInput;
import com.cloudera.batyr.mapreduce.FileOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import static com.cloudera.batyr.mapreduce.Format.*;

@FileInput(TextFile)
@FileOutput(SequenceFile)
public class InnerWordCount extends BatyrJob {

  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    Pattern delimeter = Pattern.compile("[\\s\\p{Punct}]+");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      for (String word : delimeter.split(value.toString())) {
        context.write(new Text(word), new LongWritable(1l));
      }
    }
  }

  public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      long sum = 0l;
      for (LongWritable value : values) {
        sum += value.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }
}