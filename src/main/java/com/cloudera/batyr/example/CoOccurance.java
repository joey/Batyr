package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrApplication;
import com.cloudera.batyr.mapreduce.Do;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.mapreduce.Reducer;

public class CoOccurance extends BatyrApplication {{
  
  /*
   * Convert item,user pairs to a user,item adjacency list
   */
  phase(new Do() {

    public void map(long offset, String line) {
      String[] parts = line.split(",");
      String item = parts[0];
      String user = parts[1];
      write(user, item);
    }

    public void reduce(String user, Iterable<String> items) {
      StringBuilder allItems = new StringBuilder();
      for (String item : items) {
        allItems.append(item).append(",");
      }
      allItems.deleteCharAt(allItems.length() - 1);
      write(user, allItems.toString());
    }
  });

  /*
   * Calculate set cardinality (type 0) and set intersection cardinality (type 1)
   */
  phase(new Do() {

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
  });

  /*
   * Calculate scores
   */
  phase(new Do() {

    public void map(String key, String value) {
      write(key, value);
    }

    public int getPartition(String key, String value, int numPartitions) {
      return (key.substring(0, key.indexOf(':')).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
    
    String curItem = null;
    long countA = 0;

    public void reduce(String key, Iterable<String> values) {
      int colIdx = key.indexOf(':');
      String itemA = key.substring(0, colIdx);
      String type = key.substring(colIdx + 1, colIdx + 2);
      if (!itemA.equals(curItem)) {
        curItem = itemA;
        countA = Long.parseLong(values.iterator().next());
      }

      if ("1".equals(type)) {
        for (String value : values) {
          String[] parts = value.split(",");
          String itemB = parts[0];
          long countB = Long.parseLong(parts[1]);
          long countIntersection = Long.parseLong(parts[2]);
          double score = (double) (countIntersection) / (double) (countA + countB - countIntersection);
          write(itemA + ":" + itemB, score);
        }
      }
    }
  });
  
  /*
   * Sort scores and pick out top 5 items
   */
  phase(new Do() {
    public void map(String itemPair, double score) {
      String [] parts = itemPair.split(":");
      String itemA = parts[0];
      String itemB = parts[1];
      write(String.format("%s:%012.10f:%s", itemA, 1-score, itemB), score);
    }
    
    public int getPartition(String key, double value, int numPartitions) {
      return (key.substring(0, key.indexOf(':')).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
    
    class Pair implements Comparable<Pair> {
      double score;
      String item;

      public Pair(double score, String item) {
        this.score = score;
        this.item = item;
      }

      @Override
      public int compareTo(Pair o) {
        if (o.score > score) {
          return -1;
        } else if (o.score < score) {
          return 1;
        } else {
          return item.compareTo(o.item);
        }
      }
    }
    String curItem = null;
    SortedSet<Pair> topItems = new TreeSet<Pair>();
    
    public void reduce(String key, Iterable<Double> values) {
      String itemA = key.substring(0, key.indexOf(':'));
      String itemB = key.substring(key.lastIndexOf(':')+1);
      Double score = values.iterator().next();
      if (!itemA.equals(curItem)) {
        if (topItems.size() > 0) {
          StringBuilder topItemList = new StringBuilder();
          for (Pair pair : topItems) {
            topItemList.append(pair.item).append(',');
          }
          topItemList.deleteCharAt(topItemList.length()-1);
          write(curItem, topItemList.toString());
        }
        curItem = itemA;
        topItems.clear();
      }
      
      topItems.add(new Pair(score, itemB));
      if (topItems.size() > 5) {
        topItems.remove(topItems.last());
      }
    }
    
    public void cleanup(Reducer.Context context) {
      if (topItems.size() > 0) {
        StringBuilder topItemList = new StringBuilder();
          for (Pair pair : topItems) {
            topItemList.append(pair.item).append(',');
          }
          topItemList.deleteCharAt(topItemList.length()-1);
          write(curItem, topItemList.toString());
      }
    }
  });
}}
