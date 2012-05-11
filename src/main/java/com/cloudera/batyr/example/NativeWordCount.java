package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;
import com.cloudera.batyr.mapreduce.FileInput;
import com.cloudera.batyr.mapreduce.FileOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import static com.cloudera.batyr.mapreduce.Format.*;

@FileInput(TextFile)
@FileOutput(SequenceFile)
public class NativeWordCount extends BatyrJob {

  Pattern delimeter = Pattern.compile("[\\s\\p{Punct}]+");

  public void map(long key, String value) throws IOException, InterruptedException {
    for (String word : delimeter.split(value)) {
      write(word, 1l);
    }
  }

  public void reduce(String key, Iterable<Long> values) throws IOException, InterruptedException {
    long sum = 0l;
    for (Long value : values) {
      sum += value;
    }
    write(key, sum);
  }
}