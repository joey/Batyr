package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;
import com.cloudera.batyr.mapreduce.FileInput;
import com.cloudera.batyr.mapreduce.FileOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import static com.cloudera.batyr.mapreduce.Format.*;

@FileInput(TextFile)
@FileOutput(SequenceFile)
public class ReuseWordCount extends BatyrJob {

  public static class Map extends WordTokenizer<LongWritable, Text> {
  }

  public static class Reduce extends ValueCounter<Text> {
  }
}