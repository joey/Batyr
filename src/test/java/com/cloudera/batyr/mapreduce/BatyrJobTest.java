package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.example.ReuseWordCount;
import com.cloudera.batyr.io.LongWritableValues;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;
import static org.junit.Assert.*;
import static com.cloudera.batyr.mapreduce.Format.*;

public class BatyrJobTest {

  private static Logger LOG = Logger.getLogger(BatyrJobTest.class);

  @Test
  public void testSetConf() throws IOException {
    Configuration conf = new Configuration(false);
    conf.clear();
    conf.set("string.property", "value");
    conf.setBoolean("boolean.property", true);
    conf.setLong("long.property", 42l);
    BatyrJob job = new BatyrJob() {
    };
    job.setConf(conf);
    Configuration result = job.getConf();
    assertEquals("value", result.get("string.property"));
    assertEquals(true, result.getBoolean("boolean.property", false));
    assertEquals(42l, result.getLong("long.property", 0l));
  }

  @Test
  public void testFindMapperAndReducer() throws Exception {
    LOG.debug("\n\nTest find mapper and reducer\n");
    class TestJob extends BatyrJob {

      public TestJob() throws IOException {
      }

      class MyMapper extends Mapper<Text, Text, Text, Text> {
      }

      class MyReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
      }
    }

    BatyrJob job = new TestJob();
    job.configureJob();
    assertNotSame(Mapper.class, job.getMapperClass());
    assertSame(TestJob.MyMapper.class, job.getMapperClass());
    assertNotSame(Reducer.class, job.getReducerClass());
    assertSame(TestJob.MyReducer.class, job.getReducerClass());
    assertSame(Text.class, job.getMapOutputKeyClass());
    assertSame(Text.class, job.getMapOutputValueClass());
    assertSame(LongWritable.class, job.getOutputKeyClass());
    assertSame(LongWritable.class, job.getOutputValueClass());
  }

  @Test
  public void testMultipleMappersAndReducers() throws Exception {
    LOG.debug("\n\nTest multiple mappers and reducers\n");
    BatyrJob job = new BatyrJob() {

      class MyMapper extends Mapper<Text, Text, Text, Text> {
      }

      class MyOtherMapper extends Mapper<Text, Text, Text, Text> {
      }

      class MyReducer extends Reducer<Text, Text, Text, Text> {
      }

      class MyOtherReducer extends Reducer<Text, Text, Text, Text> {
      }
    };
    job.configureJob();
    assertNotSame(Mapper.class, job.getMapperClass());
    assertNotSame(Reducer.class, job.getReducerClass());
  }

  @Test
  public void testNoReducer() throws Exception {
    LOG.debug("\n\nTest setting no reducer\n");
    BatyrJob job = new BatyrJob() {

      class MyMapper extends Mapper<Text, Text, Text, Text> {
      }
    };
    job.configureJob();

    assertEquals(0, job.getNumReduceTasks());
  }

  @FileInput(SequenceFile)
  @FileOutput(SequenceFile)
  static class JobWithInput extends BatyrJob {

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    }
  }

  @Test
  public void testInputOutput() throws Exception {
    LOG.debug("\n\nTest getting input and output formats from annotation\n");
    BatyrJob job = new JobWithInput();
    job.configureJob(new String[0]);
    assertSame(SequenceFileInputFormat.class, job.getInputFormatClass());
    assertSame(SequenceFileOutputFormat.class, job.getOutputFormatClass());
  }

  @Test
  public void testOutputWithArg() throws Exception {
    LOG.debug("\n\nTest getting output path from args\n");
    BatyrJob job = new JobWithInput();
    job.configureJob(new String[]{"-output", "output_dir"});
    assertEquals(new Path("output_dir"), FileOutputFormat.getOutputPath(job.job));
  }

  @Test
  public void testInputWithArg() throws Exception {
    LOG.debug("\n\nTest getting input path from args\n");
    BatyrJob job = new JobWithInput();
    job.configureJob(new String[]{"-input", "input_dir"});
    Set<String> paths = new HashSet<String>();
    for (Path path : FileInputFormat.getInputPaths(job.job)) {
      paths.add(path.getName());
    }
    assertTrue(paths.contains("input_dir"));
  }

  @Test
  public void testInputAndOutputWithArgs() throws Exception {
    LOG.debug("\n\nTest getting input and output paths from args\n");
    BatyrJob job = new JobWithInput();
    job.configureJob(new String[]{"-output", "output_dir", "-input", "input_dir"});
    Set<String> paths = new HashSet<String>();
    for (Path path : FileInputFormat.getInputPaths(job.job)) {
      paths.add(path.getName());
    }
    assertTrue(paths.contains("input_dir"));
    assertEquals(new Path("output_dir"), FileOutputFormat.getOutputPath(job.job));

    job = new JobWithInput();
    job.configureJob(new String[]{"-input", "input_dir", "-output", "output_dir"});
    paths = new HashSet<String>();
    for (Path path : FileInputFormat.getInputPaths(job.job)) {
      paths.add(path.getName());
    }
    assertTrue(paths.contains("input_dir"));
    assertEquals(new Path("output_dir"), FileOutputFormat.getOutputPath(job.job));
  }

  @FileInput(SequenceFile)
  @FileOutput(SequenceFile)
  static class JobWithMapAndReduceMethods extends BatyrJob {

    public void map(LongWritable key, Text value) throws IOException, InterruptedException {
    }

    public void reduce(Text key, LongWritableValues values) throws IOException, InterruptedException {
    }
  }

  @Test
  public void testImplicitMapperAndReducer() throws Exception {
    LOG.debug("\n\nTest a job with implicit map() and reduce() methods\n");
    BatyrJob job = new JobWithMapAndReduceMethods();
    job.configureJob(new String[0]);
    assertNotSame(Mapper.class, job.getMapperClass());
    assertSame(WritableMapper.class, job.getMapperClass());
    assertNotSame(Reducer.class, job.getReducerClass());
    assertSame(WritableReducer.class, job.getReducerClass());
    assertSame(Text.class, job.getMapOutputKeyClass());
    assertSame(LongWritable.class, job.getMapOutputValueClass());
  }
  
  @Test
  public void testReusedMapperAndReducer() throws Exception {
    LOG.debug("\n\nTest a job that reuses an existing, generic Mapper and Reducer\n");
    BatyrJob job = new ReuseWordCount();
    job.configureJob(new String[0]);
    assertNotSame(Mapper.class, job.getMapperClass());
    assertSame(ReuseWordCount.Map.class, job.getMapperClass());
    assertNotSame(Reducer.class, job.getReducerClass());
    assertSame(ReuseWordCount.Reduce.class, job.getReducerClass());
    assertSame(Text.class, job.getMapOutputKeyClass());
    assertSame(LongWritable.class, job.getMapOutputValueClass());
  }
}
