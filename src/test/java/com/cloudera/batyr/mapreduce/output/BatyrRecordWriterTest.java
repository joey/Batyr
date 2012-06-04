package com.cloudera.batyr.mapreduce.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.*;

public class BatyrRecordWriterTest {

  @Test
  public void testWrite() throws Exception {
    OutputFormat format = new NullOutputFormat();
    TaskAttemptContext context = new TaskAttemptContext(new Configuration(), new TaskAttemptID(new TaskID(new JobID("jt", 0), true, 0), 0));
    Object key = new Text("key");
    Object value = new Text("value");
    BatyrRecordWriter instance = new BatyrRecordWriter(format, context);
    instance.write(key, value);
    instance.write(key, value);
    instance.close(context);
  }

  @Test
  public void testClose() throws Exception {
    OutputFormat format = new NullOutputFormat();
    TaskAttemptContext context = new TaskAttemptContext(new Configuration(), new TaskAttemptID(new TaskID(new JobID("jt", 0), true, 0), 0));
    BatyrRecordWriter instance = new BatyrRecordWriter(format, context);
    instance.close(context);
  }

}
