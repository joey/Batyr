package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.mapreduce.input.NullInputFormat;
import java.util.EnumMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public enum Format {

  TextFile,
  KeyValueFile,
  SequenceFile,
  DevNull;
  private static final Map<Format, Class<? extends InputFormat>> formatToInput;
  private static final Map<Format, Class<? extends OutputFormat>> formatToOutput;

  static {
    formatToInput = new EnumMap<Format, Class<? extends InputFormat>>(Format.class);
    formatToInput.put(TextFile, TextInputFormat.class);
    formatToInput.put(KeyValueFile, KeyValueTextInputFormat.class);
    formatToInput.put(SequenceFile, SequenceFileInputFormat.class);
    formatToInput.put(DevNull, NullInputFormat.class);

    formatToOutput = new EnumMap<Format, Class<? extends OutputFormat>>(Format.class);
    formatToOutput.put(TextFile, TextOutputFormat.class);
    formatToOutput.put(KeyValueFile, TextOutputFormat.class);
    formatToOutput.put(SequenceFile, SequenceFileOutputFormat.class);
    formatToOutput.put(DevNull, NullOutputFormat.class);
  }

  public static Class<? extends InputFormat> getInputFormat(FileInput input) {
    return formatToInput.get(input.value());
  }

  public static Class<? extends OutputFormat> getOutputFormat(FileOutput output) {
    return formatToOutput.get(output.value());
  }
}
