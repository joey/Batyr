package com.cloudera.batyr.example;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class WordTokenizer<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, Text, LongWritable> {

  private static Logger LOG = Logger.getLogger(WordTokenizer.class);
  Pattern delimeter = Pattern.compile("[\\s\\p{Punct}]+");

  @Override
  protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
    for (String word : delimeter.split(value.toString())) {
      context.write(new Text(word), new LongWritable(1l));
    }
  }
}
