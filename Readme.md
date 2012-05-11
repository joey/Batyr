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
	import java.io.IOException;
	
	public class WordCount extends BatyrJob {
	
	  public void map(long key, String value) throws IOException, InterruptedException {
	    for (String word : value.split("[\\s\\p{Punct}]+")) {
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

That's it. Only two imports and regular Java types for your input and output data.
It's only 21 lines (including blanks, imports and package declaration), which is small
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

### Native Types and Implicit Functions

TODO

### Using Writables

TODO

### Defining Custom Mapper and Reducer Classes

TODO

### Using a Pre-existing Mapper and Reducer Class

TODO

### Custom Configuration

TODO

## Future Development

### Additional Input and Output Formats

Batyr supports some of the common file formats for input and output,
including text files, sequence files, and map files. Batyr will be adding support
for Avro data files in the near future and hopes to add other input and output
sources such as HBase and HCatalog tables down the road.

### Multi-round Jobs

Batyr only lets you define a single round of MapReduce at the moment. A lot of
the more complex use cases that run on Hadoop require multiple rounds of
map and reduce. We plan on adding features to make it as simple as possible to build
these multi-round jobs without having to repeat yourself.

### Additional Languages

Batyr is written in Java, the native language for Hadoop, and unlike many Java APIs
is devoid of boiler plate code and complex design patterns. This makes Batyr a natural
fit for other JVM-compatabile languages such as Jython and JRuby. We hope to extends
the Batyr API so you can use your favorite language.
