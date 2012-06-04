package com.cloudera.batyr.mapreduce.output;

import com.cloudera.batyr.mapreduce.FileOutput;
import com.cloudera.batyr.mapreduce.Format;
import java.lang.annotation.Annotation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BatyrOutputFormatTest {

  @Test
  public void testSetOutputFormat() throws Exception {
    JobContext job = new Job();
    FileOutput output = new FileOutput() {

      @Override
      public Format value() {
        return Format.SequenceFile;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return Annotation.class;
      }

    };
    BatyrOutputFormat.setOutputFormat(job, output);

    assertEquals(SequenceFileOutputFormat.class.getName(),
        job.getConfiguration().get(BatyrOutputFormat.class.getName() + ".output.format"));
  }

  @Test
  public void testGetRecordWriter() throws Exception {
    Job job = new Job();
    FileOutput output = new FileOutput() {

      @Override
      public Format value() {
        return Format.SequenceFile;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return Annotation.class;
      }

    };
    BatyrOutputFormat.setOutputFormat(job, output);
    SequenceFileOutputFormat.setOutputPath(job, new Path("out"));

    TaskAttemptContext context = null;
    BatyrOutputFormat instance = new BatyrOutputFormat();
    instance.checkOutputSpecs(job);
    RecordWriter result = instance.getRecordWriter(context);
    assertEquals(BatyrRecordWriter.class, result.getClass());

    instance.checkOutputSpecs(job);
    result = instance.getRecordWriter(context);
    assertEquals(BatyrRecordWriter.class, result.getClass());
  }

  @Test
  public void testGetOutputCommitter() throws Exception {
    Job job = new Job();
    FileOutput output = new FileOutput() {

      @Override
      public Format value() {
        return Format.SequenceFile;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return Annotation.class;
      }

    };
    BatyrOutputFormat.setOutputFormat(job, output);
    SequenceFileOutputFormat.setOutputPath(job, new Path("out"));

    TaskAttemptContext context = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID(new TaskID(new JobID("jt", 0), true, 0), 0));
    BatyrOutputFormat instance = new BatyrOutputFormat();
    OutputCommitter result = instance.getOutputCommitter(context);
    assertEquals(FileOutputCommitter.class, result.getClass());

    result = instance.getOutputCommitter(context);
    assertEquals(FileOutputCommitter.class, result.getClass());
  }

  @Test
  public void testGetConf() {
    BatyrOutputFormat instance = new BatyrOutputFormat();
    Configuration expResult = new Configuration();
    instance.setConf(expResult);
    Configuration result = instance.getConf();
    assertEquals(expResult, result);
  }

}
