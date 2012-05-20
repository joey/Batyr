package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;
import com.cloudera.batyr.mapreduce.FileInput;
import com.cloudera.batyr.mapreduce.Format;
import org.apache.log4j.Logger;

@FileInput(Format.KeyValueFile)
public class SetCount extends BatyrJob {

  private static Logger LOG = Logger.getLogger(SetCount.class);

  public void map(String user, String itemString) {
    String[] items = itemString.split(",");
    for (String itemA : items) {
      write(itemA + ":0", 1l);
      for (String itemB : items) {
        if (!itemA.equals(itemB)) {
          write(itemA + ":1:" + itemB, 1l);
        }
      }
    }
  }

  public int getPartition(String key, long value, int numPartitions) {
    return (key.substring(0, key.indexOf(':')).hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
  
  String curItem = null;
  long curCount = 0;
  public void reduce(String key, Iterable<Long> counts) {
    int colIdx = key.indexOf(':');
    String itemA = key.substring(0, colIdx);
    String type = key.substring(colIdx + 1, colIdx + 2);
    if (!itemA.equals(curItem)) {
      curItem = itemA;
      curCount = 0;
    }

    if ("0".equals(type)) {
      for (Long count : counts) {
        curCount += count;
      }
      write(curItem + ":0", curCount);
    } else if ("1".equals(type)) {
      String itemB = key.substring(colIdx + 3);
      long sum = 0;
      for (Long count : counts) {
        sum += count;
      }
      write(itemB + ":1", itemA + "," + curCount + "," + sum);
    }
  }
}
