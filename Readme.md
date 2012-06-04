# Batyr - The easy way to speak MapReduce

## Introduction

Batyr is a Java (for now) API that lets you write Hadoop MapReduce jobs
without all of the manual configuration that's required by Hadoop's built-in
APIs.

## Getting Started

Batyr uses Apache Maven to manage dependencies, build its source, and
produce an executable jar. If you want to run the built-in word count
example, type the following commands:

	mvn install
	hadoop jar target/batyr-0.0.1-SNAPSHOT.jar com.cloudera.batyr.example.WordCount -input <input directory> -output <output directory>

## Hello World

The canonical hello world application for Hadoop is word count, so lets start there.
This is what word count written with Batyr looks like:

	package com.cloudera.batyr.example;

	import com.cloudera.batyr.mapreduce.BatyrJob;
	
	public class WordCount extends BatyrJob {
	
	  public void map(long key, String value) {
	    for (String word : value.split("[\\s\\p{Punct}]+")) {
	      write(word, 1l);
	    }
	  }
	
	  public void reduce(String key, Iterable<Long> values) {
	    long sum = 0l;
	    for (Long value : values) {
	      sum += value;
	    }
	    write(key, sum);
	  }
	}

That's it. Only two imports and regular Java types for your input and output data.
It's only 20 lines (including blanks, imports and package declaration), which is small
enough to fit on a standard VT100.

This example shows the simplest way to write a MapReduce job with Batyr, but it's not
the only way. We'll go over the variants of the API a little bit later, but for now
we can dissect this example. The first thing you'll notice is that your job needs to
extends the BatyrJob class. By extending this class, your job will be configured
automatically including all necessary settings for input and output types. You'll
also notice that we didn't specify a Mapper or Reducer class. Instead, we just wrote
simple map() and reduce() methods. Batyr automatically detects these methods and
configures Hadoop to call them during the map and reduce phases, respectively.

## Why Batyr?

There are tons of high-level languages that you can use to process data with Hadoop.
The two most popular are Apache Pig and Apache Hive. The difference between Batyr and
these languages is they try to hide MapReduce from you, Batyr puts it front and center.
There's nothing wrong with the MapReduce approach to building applications and for a
number of use cases, it's easier to express your solution in MapReduce than it is in
these high level languages.

Where Hadoop fails is in the complexity of its API. The standard Hadoop word count
example is 58 lines of code with 12 of those lines being purely for configuration
and another 7 in import statements (and that's with importing package wild cards!).
Batyr lets you stick with the MapReduce style, but leave the boilerplate at home.

Batyr isn't the only Java API built on top of Hadoop. Other common examples here are
Crunch and Cascading. Both are nice APIs and add a lot of functionality and ease of
use to Hadoop. However, they both introduce their own models of computation. Like
Hive or Pig, your application isn't written as a series of map and reduce functions,
but in a pipeline custom operators.

Batyr doesn't replace any of these other languages or APIs, it simply gives you the
ability to write MapReduce applications in the simplest terms possible.

## Batyr API

### Native Types and Implicit Mapper and Reducer Classes

The simplest type of BatyrJob uses implicitly defined Mapper and Reducer classes.

If you create a method in your job class called map() that takes in two parameters,
then your Mapper class will automatically be set to NativeTypeMapper which will call
your map() method for each input key, value pair. NativeTypeMapper will also try to
automatically translate the input types (often writables) to native types used for
your map() method. This why you could take in a long and String in the WordCount
example: 

	public class WordCount extends BatyrJob {
	
	  public void map(long key, String value) {
	    for (String word : value.split("[\\s\\p{Punct}]+")) {
	      write(word, 1l);
	    }
	  }
	  ...
	}

Similarly, if you create a method in your job class called reduce() that takes two
parameters, the second being an Iterable, then your Reducer class will be automatically
set to NativeTypeReducer which will call your reduce() method for each key and
Iterable of values. Batyr automatically configures the KeyWritable and ValueWritable
classes as the output from your Mapper. These classes support automatic serialization of
primitive java types and their capital equivalences (e.g. long and Long) as well as
Strings, Lists, Maps and any Class that implements Serializable. Note, Serializable
serializes the full package and class name with every object, so it's best not to use it
for performance sensative code. Again, this was the technique used in the WordCount
example:

	public class WordCount extends BatyrJob {
	  ...
	  public void reduce(String key, Iterable<Long> values) {
	    long sum = 0l;
	    for (Long value : values) {
	      sum += value;
	    }
	    write(key, sum);
	  }
	}

This style of writing jobs is the easiest and puts the least amount of constraints on
the developer. If you're just starting out and are new to MapReduce and Batyr, this is
where you want to be. However, this style is not designed for maximum performance.

### Using Writables

If you are at all performance conscience, then you may want to use Writables instead of
native Java types. Writables are generally more efficient to serialize/deserialize and
are mutable which lowers the number of object creations generally required. Fortunately,
Batyr makes writing MapReduce jobs that use Writables just as easy as it does with Java
native types. Let's see the Writables version of our word count example:

	package com.cloudera.batyr.example;
	
	import com.cloudera.batyr.io.LongWritableValues;
	import com.cloudera.batyr.mapreduce.BatyrJob;
	import com.cloudera.batyr.mapreduce.FileInput;
	import com.cloudera.batyr.mapreduce.FileOutput;
	import java.io.IOException;
	import java.util.regex.Pattern;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import static com.cloudera.batyr.mapreduce.Format.*;
	
	@FileInput(TextFile)
	@FileOutput(SequenceFile)
	public class SimpleWordCount extends BatyrJob {
	
	  Pattern delimeter = Pattern.compile("[\\s\\p{Punct}]+");
	
	  public void map(LongWritable key, Text value) throws IOException, InterruptedException {
	    for (String word : delimeter.split(value.toString())) {
	      write(new Text(word), new LongWritable(1l));
	    }
	  }
	
	  public void reduce(Text key, LongWritableValues values) throws IOException, InterruptedException {
	    long sum = 0l;
	    for (LongWritable value : values) {
	      sum += value.get();
	    }
	    write(key, new LongWritable(sum));
	  }
	}

Once again, the job is declared as a single class with appropriately named map() and
reduce methods. The only thing that may look unusual is the use of the
LongWritableValues class in place of the typical Iterable<LongWritable>. This is
required to get around a limitation of Java's reflection support. Namely, Java
does not keep the paramaterized types of variables at runtime. What this means is that
the type Iterable<LongWritable> and Iterable<Text> are completely indistinguishable at
runtime. In order to correctly set the map phase output value type (required by Hadoop),
Batyr needs this information. You should see a number of these Iterable classes
pre-built for the common Writable types. If you need to use a custom Writable class
for the output values of your map phase, you'll need to implement your own Values
class. To make this easier, Batyr comes with a base class called WritableValues that
simplifies the create of these helper classes. For example, suppose you have a class
called MyWritable. You could implement the MyWritableValues class like so:

	package com.cloudera.batyr.example;
	
	import com.cloudera.batyr.io.WritableValues;
	
	public class MyWritableValues extends WritableValues<MyWritable> {
	
	  public MyWritableValues(Iterable<MyWritable> iterable) {
	    super(iterable);
	  }
	
	}

The only requirement is that you build a single-argument constructor that takes an
Iterable<V extends Writable>. Alternatively, you can use a plain Iterable in your
reduce method and simply set the map output value class in your configureManually()
method:

	public class MyJob extends BatyrJob {
	  ...
	  @Override
	  protected String[] configureManually(String[] args) throws Exception {
	    setMapOutputValueClass(MyWritable.class);
	    return args;
	  }
	  ...
	}

### Defining Custom Mapper and Reducer Classes

TODO

### Using a Pre-existing Mapper and Reducer Class

TODO

### Custom Configuration

TODO

### Multi-round Applications

More complex applications require multiple rounds of MapReduce. With the standard
Hadoop APIs this is cumbersome the developer is required to build all of the rounds as
independent jobs, manage the wiring between rounds by setting the appropriate input
and output directories and run the jobs sequentially. Batyr supports building simple
pipelines of MapReduce jobs (single dependency chain, no branching) without extraneous
configuration or manual job invocation. Let's take a look at the multi-round application
support by starting with an example of calculating related items based on Co-Occurance:

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

Just as in previous examples, you'll see that the vast majority of the code is the
application logic being implemented. The major departure from the single job examples
is the use of the BatyrApplication class and the creation of individual jobs (called
phases) as anonymous inner classes. Also notice the double braces at the beginning
({{) and end (}}). These are requried becuase we're actually building these phases
inside an object initializer.

Object initializers are a Java feature, similar to static initializers, that are
executed every time an instance of class is initialized. You can think of it as a
code block that get's copied into every constructor. Batyr uses the object initializer
as a form of syntactic sugar. Let's take a closer look:

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
	  ...
	}}

You'll see that we're calling the phase() method to define the next phase of MapReduce.
The framework is able to capture the order of the phases based on the order of the calls
to phase(). Each phase takes an instance of a Do object. The Do class is any empty extender
of the BatyrJob class and is provided as additional syntactic sugare to imply what's happening
under the covers (i.e. for this phase, do the following).

TODO

## Future Development

### Additional Input and Output Formats

Batyr supports some of the common Hadoop file formats for input and output,
including text files, sequence files, and map files. Batyr will be adding support
for Avro data files in the near future and we hope to add other input and output
sources such as HBase and HCatalog tables down the road.

### Additional Languages

Batyr is written in Java, the native language for Hadoop and, unlike many Java APIs,
is devoid of boiler plate code and complex design patterns. This makes Batyr a natural
fit for other JVM-compatabile languages such as Jython and JRuby. We hope to extend
the Batyr API so you can use your favorite language.
