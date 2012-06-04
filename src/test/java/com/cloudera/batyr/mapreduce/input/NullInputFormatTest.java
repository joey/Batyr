package com.cloudera.batyr.mapreduce.input;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import static org.junit.Assert.*;
import org.junit.Test;

public class NullInputFormatTest {

  @Test
  public void testGetSplits() throws Exception {
    JobContext context = null;
    NullInputFormat instance = new NullInputFormat();
    List expResult = Arrays.asList();
    List result = instance.getSplits(context);
    assertEquals(expResult, result);
  }

  @Test
  public void testCreateRecordReader() throws Exception {
    InputSplit split = null;
    TaskAttemptContext context = null;
    NullInputFormat instance = new NullInputFormat();
    RecordReader result = instance.createRecordReader(split, context);
    result.initialize(split, context);
    assertEquals(false, result.nextKeyValue());
    assertNull(result.getCurrentKey());
    assertNull(result.getCurrentValue());
    assertEquals(1.0f, result.getProgress(), 0.00001f);
    result.close();
  }

}
