package com.cloudera.batyr.mapreduce.output;

import com.cloudera.batyr.mapreduce.FileOutput;
import com.cloudera.batyr.mapreduce.Format;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class BatyrOutputFormat<K, V> extends OutputFormat<K, V> implements Configurable {

  private static Logger LOG = Logger.getLogger(BatyrOutputFormat.class);
  private final static String OUTPUT_CLASS_KEY = BatyrOutputFormat.class.getName() + ".output.format";
  private Configuration conf;
  private OutputFormat<K, V> output;
  public BatyrOutputFormat() {
  }

  public static void setOutputFormat(JobContext job, FileOutput output) {
    job.getConfiguration().setClass(OUTPUT_CLASS_KEY, Format.getOutputFormat(output), OutputFormat.class);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BatyrRecordWriter<K, V>(output, context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    if (output == null) {
      setConf(context.getConfiguration());
    }
    output.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    if (output == null) {
      setConf(context.getConfiguration());
    }
    return output.getOutputCommitter(context);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    output = ReflectionUtils.newInstance(conf.getClass(OUTPUT_CLASS_KEY, TextOutputFormat.class, OutputFormat.class), conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
