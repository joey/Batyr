package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.mapreduce.output.BatyrOutputFormat;
import com.cloudera.batyr.parser.SimpleCommandLineParser;
import com.cloudera.batyr.types.KeyWritable;
import com.cloudera.batyr.types.ValueWritable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public abstract class BatyrJob implements Tool, Delegator {

  private final static Logger LOG = Logger.getLogger(BatyrJob.class);
  private final static String DELEGATOR_CLASS_KEY = BatyrJob.class.getName() + ".delegator.class";
  protected Configuration conf;
  protected String jobName;
  protected Job job;
  private TaskInputOutputContext context;
  /**
   * Construct a new job with the given configuration and job name.
   *
   * @param conf    The initial configuration for the job.
   * @param jobName The name of the job. If null, the job name will be set
   * to the simple name of the job class.
   */
  protected BatyrJob(Configuration conf, String jobName) {
    this.conf = conf;
    this.jobName = (jobName != null ? jobName : getClass().getSimpleName());
    try {
      job = new Job(this.conf, this.jobName);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Construct a new job with the given configuration. The job name will be
   * set to the simple name of the job class.
   *
   * @param conf The initial configuration for the job.
   */
  protected BatyrJob(Configuration conf) {
    this(conf, null);
  }

  /**
   * Construct a new job with the default configuration. The job name will be
   * set to the simple name of the job class.
   */
  public BatyrJob() {
    this(new Configuration());
  }

  // <editor-fold defaultstate="collapsed" desc="Methods from Configurable">
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  // </editor-fold>

  // <editor-fold defaultstate="collapsed" desc="Job configuration and execution methods">
  /**
   * Create a new instance of this job and use {@link ToolRunner} to execute the
   * job.
   *
   * @param args The commandline arguments to the job.
   */
  public static void main(String[] args) {
    String jobClassName = getJobClassName();
    try {
      Class<? extends BatyrJob> jobClass = Class.forName(jobClassName).asSubclass(BatyrJob.class);
      BatyrJob job = jobClass.newInstance();
      System.exit(ToolRunner.run(job, args));
    } catch (ClassNotFoundException ex) {
      LOG.error("Can't find your job class (" + jobClassName + "), how did you execute main()?", ex);
    } catch (IllegalAccessException ex) {
      LOG.error("No public noargs constructor for your job class (" + jobClassName + ")", ex);
    } catch (InstantiationException ex) {
      LOG.error("Can't instantiate your job class (" + jobClassName + ")", ex);
    } catch (Exception ex) {
      LOG.error("Error trying to run your job", ex);
    }
  }

  /**
   * Find the name of class we called main() on. This method relies on the
   * sun.java.command system property and may fail on older JVMs or JVMs
   * from other vendors.
   *
   * @return The name of the class we called main() on.
   */
  private static String getJobClassName() {
    String command = System.getProperty("sun.java.command");
    if (command == null) {
      NullPointerException ex = new NullPointerException("System property sun.java.command is null");
      LOG.error("Your JVM doesn't set sun.java.command, you're BatyrJob will have to define it's own main() method", ex);
      throw ex;
    }
    String[] parts = command.split(" ");
    if (parts[0].startsWith("org.apache.hadoop.util.RunJar")) {
      // org.apache.hadoop.util.RunJar <jar file>[ <main class>][ <args>...]
      String mainClassName = null;
      String jarFileName = parts[1];

      try {
        JarFile jarFile;
        try {
          jarFile = new JarFile(jarFileName);
        } catch (IOException io) {
          throw new IOException("Error opening job jar: " + jarFileName, io);
        }

        Manifest manifest = jarFile.getManifest();
        if (manifest != null) {
          mainClassName = manifest.getMainAttributes().getValue("Main-Class");
        }
        jarFile.close();
      } catch (IOException ex) {
        LOG.warn("Couldn't read manifest from jar file " + jarFileName, ex);
      }

      if (mainClassName == null) {
        mainClassName = parts[2];
      }
      mainClassName = mainClassName.replaceAll("/", ".");
      return mainClassName;
    } else {
      // <main class>[ <args>...]
      return parts[0];
    }
  }

  /**
   * Configure and run the job on the cluster.
   *
   * @param args  The commandline arguments for the job.
   * @return      1 if the job failed, 0 otherwise.
   * @throws Exception An error occured configuring or running the job.
   */
  @Override
  public int run(String[] args) throws Exception {
    configureJob(args);
    if (!waitForCompletion(true)) {
      return 1;
    }

    return 0;
  }

  /**
   * Subclasses extending {@link BatyrJob} should override this method if they
   * want to modify the configuration of the job after automatic configuration,
   * but before job submission.
   *
   * @param args  The commandline arguments for the job.
   * @return      The unused commandline arguments.
   * @throws Exception AN error occured configuring the job.
   */
  protected String[] configureManually(String[] args) throws Exception {
    return args;
  }

  /**
   * Configure the job with no commandline arguments.
   *
   * @return  An empty array of strings.
   * @throws Exception
   */
  String[] configureJob() throws Exception {
    return configureJob(new String[0]);
  }

  /**
   * Configure the job.
   *
   * @param args  The commandline arguments.
   * @return      The unused commandline arguments.
   * @throws Exception
   */
  String[] configureJob(String[] args) throws Exception {
    args = configureAutomatically(args);
    args = configureManually(args);

    if (getReducerClass().equals(Reducer.class)) {
      LOG.debug("No reducer class configured, setting number of reduce tasks to 0");
      setNumReduceTasks(0);
    } else if (getNumReduceTasks() == -1) {
      JobClient client = new JobClient(new JobConf(conf));
      setNumReduceTasks(client.getDefaultReduces() / 2);
      LOG.warn("Number of reducers was not set, automatically configuring to " + client.getDefaultReduces() / 2);
    }
    return args;
  }

  /**
   * Use reflection to automatically configure the job.
   *
   * Automatic configuration supports the following commandline arguments:
   *
   *  -input  The input directory or file for the job.
   *  -output The output directory for the job. The output directory must not
   *          exist.
   *
   * @param args The commandline arguments.
   * @return The unused commandline arguments.
   * @throws Exception An error occured while configuring the job.
   */
  private String[] configureAutomatically(String[] args) throws Exception {
    setJarByClass(getClass());
    setDelegatorClass(job, getClass());
    args = setInput(args);
    args = setTasks(args);
    setNumReduceTasks(-1);
    args = setOutput(args);

    return args;
  }

  /**
   * Set the input format based on the annotation on the job class and the
   * input directory based on the commandline arguments.
   *
   * @param args  The commandline arguments.
   * @return      The unused commandline arguments.
   * @throws Exception An error occured trying to configure the input.
   */
  private String[] setInput(String[] args) throws Exception {
    Map<String, String> settings = new HashMap<String, String>();
    args = SimpleCommandLineParser.parse(args, new String[]{"input"}, new String[0], settings);
    String in = settings.get("input");
    if (in != null) {
      FileInputFormat.addInputPath(job, new Path(in));
    }

    FileInput input = getClass().getAnnotation(FileInput.class);
    if (input != null) {
      setInputFormatClass(Format.getInputFormat(input));
    }
    return args;
  }

  /**
   * Automatically find the map an reduce tasks and configure the output key
   * and value classes appropriately.
   *
   * The tasks can be set in one of two ways:
   *
   *  1. Have static inner class that extends Mapper/Reducer.
   *  2. Have appropriately named methods (map, combine, reduce, and/or cleanup)
   *
   * @param args  The commandline arguments.
   * @return      The unused commandline arguments.
   * @throws Exception  An error occured while configuring the tasks.
   */
  private String[] setTasks(String[] args) throws Exception {
    boolean foundMapper = false;
    boolean foundCombiner = false;
    boolean foundReducer = false;
    for (Class<?> innerClass : getClass().getDeclaredClasses()) {
      if (Mapper.class.isAssignableFrom(innerClass)) {
        if (foundMapper) {
          LOG.warn(String.format("Already set mapper to %s, ignoring other mapper %s",
              getMapperClass().getSimpleName(), innerClass.getSimpleName()));
        } else {
          LOG.debug(String.format("Found mapper %s", innerClass.getName()));
          Class<? extends Mapper> mapperClass = innerClass.asSubclass(Mapper.class);
          setMapOutputTypes(mapperClass);
          setMapperClass(mapperClass);
          foundMapper = true;
        }
      }

      if (Reducer.class.isAssignableFrom(innerClass)) {
        if (foundReducer) {
          LOG.warn(String.format("Already set reducer to %s, ignoring other reducer %s",
              getMapperClass().getSimpleName(), innerClass.getSimpleName()));
        } else {
          LOG.debug(String.format("Found reducer %s", innerClass.getName()));
          Class<? extends Reducer> reducerClass = innerClass.asSubclass(Reducer.class);
          setReduceOutputTypes(reducerClass);
          setReducerClass(reducerClass);
          foundReducer = true;
        }
      }
    }

    for (Method method : getClass().getDeclaredMethods()) {
      if (method.getName().equals("map") && method.getParameterTypes().length == 2) {
        if (foundMapper) {
          LOG.warn(String.format("Already set mapper to %s, ignoring map() method",
              getMapperClass().getSimpleName()));
        } else {
          if (Writable.class.isAssignableFrom(method.getParameterTypes()[0])) {
            setMapperClass(WritableMapper.class);
            foundMapper = true;
          } else {
            setMapperClass(NativeTypeMapper.class);
            setMapOutputKeyClass(KeyWritable.class);
            setMapOutputValueClass(ValueWritable.class);
            foundMapper = true;
          }
        }
      }

      if (method.getName().equals("combine") && method.getParameterTypes().length == 2) {
        if (foundCombiner) {
          LOG.warn(String.format("Already set combiner to %s, ignoring combine() method",
              getMapperClass().getSimpleName()));
        } else {
          if (Writable.class.isAssignableFrom(method.getParameterTypes()[0])) {
            setCombinerClass(WritableCombiner.class);
            setMapOutputKeyClass(method.getParameterTypes()[0]);
            Class<?> valuesClass = method.getParameterTypes()[1];
            ParameterizedType valuesType = (ParameterizedType) valuesClass.getGenericSuperclass();
            setMapOutputValueClass((Class<?>) valuesType.getActualTypeArguments()[0]);
          } else {
            setCombinerClass(NativeTypeCombiner.class);
          }
        }
      }

      if (method.getName().equals("reduce") && method.getParameterTypes().length == 2) {
        if (foundReducer) {
          LOG.warn(String.format("Already set reducer to %s, ignoring reduce() method",
              getMapperClass().getSimpleName()));
        } else {
          if (Writable.class.isAssignableFrom(method.getParameterTypes()[0])) {
            setReducerClass(WritableReducer.class);
            setMapOutputKeyClass(method.getParameterTypes()[0]);
            Class<?> valuesClass = method.getParameterTypes()[1];
            ParameterizedType valuesType = (ParameterizedType) valuesClass.getGenericSuperclass();
            setMapOutputValueClass((Class<?>) valuesType.getActualTypeArguments()[0]);
          } else {
            setReducerClass(NativeTypeReducer.class);
            setOutputKeyClass(KeyWritable.class);
            setOutputValueClass(ValueWritable.class);
          }
        }
      }

      if (method.getName().equals("getPartition") && method.getParameterTypes().length == 3) {
        setPartitionerClass(BatyrPartitioner.class);
      }
    }
    return args;
  }

  /**
   * Set the output format based on the annotation on the job class and the
   * output directory based on the commandline arguments.
   *
   * @param args  The commandline arguments.
   * @return      The unused commandline arguments.
   * @throws Exception An error occured trying to configure the output.
   */
  private String[] setOutput(String[] args) throws Exception {
    Map<String, String> settings = new HashMap<String, String>();
    args = SimpleCommandLineParser.parse(args, new String[]{"output"}, new String[0], settings);
    String path = settings.get("output");
    if (path != null) {
      FileOutputFormat.setOutputPath(job, new Path(path));
    } else {
      LOG.warn("No output directory specified");
    }

    FileOutput output = getClass().getAnnotation(FileOutput.class);
    if (output != null) {
      if (WritableReducer.class.equals(getReducerClass())) {
        setOutputFormatClass(BatyrOutputFormat.class);
        BatyrOutputFormat.setOutputFormat(job, output);
      } else {
        setOutputFormatClass(Format.getOutputFormat(output));
      }
    }

    return args;
  }

  /**
   * Use reflection to automatically set the map output types based on the given
   * Mapper class.
   *
   * @param mapperClass The class for the map tasks.
   */
  private void setMapOutputTypes(Class<? extends Mapper> mapperClass) {
    Class<? extends Mapper> superClass = mapperClass;
    Type[] actualTypes = null;
    Map<String, Type> nameToType = new HashMap<String, Type>();

    do {
      ParameterizedType mapperType = (ParameterizedType) superClass.getGenericSuperclass();
      actualTypes = mapperType.getActualTypeArguments();
      for (int i = 0; i < actualTypes.length; i++) {
        if (actualTypes[i] instanceof TypeVariable) {
          actualTypes[i] = nameToType.get(((TypeVariable) actualTypes[i]).getName());
        }
      }
      superClass = (Class<? extends Mapper>) superClass.getSuperclass();
      TypeVariable[] typeParameters = superClass.getTypeParameters();
      nameToType.clear();
      for (int i = 0; i < actualTypes.length; i++) {
        nameToType.put(typeParameters[i].getName(), actualTypes[i]);
      }
    } while (superClass.equals(Mapper.class) == false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("\n\n" + superClass.getName() + Arrays.toString(actualTypes).replaceAll("\\[", "<").replaceAll("]", ">") + "\n\n");
    }

    // { inputKey, inputValue, outputKey, outputValue }
    setMapOutputKeyClass((Class<?>) actualTypes[2]);
    setMapOutputValueClass((Class<?>) actualTypes[3]);
  }

  /**
   * Use reflection to automatically set the reduce output types based on the
   * given Reducer class.
   *
   * @param reducerClass The class for the reduce tasks.
   */
  private void setReduceOutputTypes(Class<? extends Reducer> reducerClass) {
    Class<? extends Reducer> superClass = reducerClass;
    Type[] actualTypes = null;
    Map<String, Type> nameToType = new HashMap<String, Type>();

    do {
      ParameterizedType reducerType = (ParameterizedType) superClass.getGenericSuperclass();
      actualTypes = reducerType.getActualTypeArguments();
      for (int i = 0; i < actualTypes.length; i++) {
        if (actualTypes[i] instanceof TypeVariable) {
          actualTypes[i] = nameToType.get(((TypeVariable) actualTypes[i]).getName());
        }
      }
      superClass = (Class<? extends Reducer>) superClass.getSuperclass();
      TypeVariable[] typeParameters = superClass.getTypeParameters();
      nameToType.clear();
      for (int i = 0; i < actualTypes.length; i++) {
        nameToType.put(typeParameters[i].getName(), actualTypes[i]);
      }
    } while (superClass.equals(Reducer.class) == false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("\n\n" + superClass.getName() + Arrays.toString(actualTypes).replaceAll("\\[", "<").replaceAll("]", ">") + "\n\n");
    }

    // { inputKey, inputValue, outputKey, outputValue }
    Type[] inputOutputTypes = actualTypes;
    setOutputKeyClass((Class<?>) inputOutputTypes[2]);
    setOutputValueClass((Class<?>) inputOutputTypes[3]);
  }
  // </editor-fold>

  // <editor-fold defaultstate="collapsed" desc="Mapper and Reducer delegation methods">
  private Method combine = null;
  private SortedMap<Object, List<Object>> collector = null;
  private int numCollected;
  private boolean combining = false;
  /**
   * Set the in memory combiner to the given method. The method must be a
   * method on this job class.
   *
   * @param combine The method for combining.
   */
  void setInMemoryCombiner(Method combine) {
    this.combine = combine;
    collector = new TreeMap<Object, List<Object>>();
    resetInMemoryCombiner();
  }

  /**
   * Reset the state of the in memory combiner. This method is called after
   * every time the in memory combiner is run.
   */
  private void resetInMemoryCombiner() {
    collector.clear();
    numCollected = 0;
    combining = false;
  }

  /**
   * Write the given key, value pair to the job's current context. If a combiner
   * is set, collection the key, value pair in memory for later combining.
   *
   * @param key   The key to write
   * @param value The value to write
   */
  protected void write(Object key, Object value) {
    if (context instanceof Reducer.Context) {
      if (context.getOutputKeyClass() == KeyWritable.class) {
        key = new KeyWritable((Comparable) key);
      }
      if (context.getOutputValueClass() == ValueWritable.class) {
        value = new ValueWritable(value);
      }
    } else if (context instanceof Mapper.Context) {
      if (combine != null && combining == false) {
        collect(key, value);
        return;
      } else {
        if (context.getMapOutputKeyClass() == KeyWritable.class) {
          key = new KeyWritable((Comparable) key);
        }
        if (context.getMapOutputValueClass() == ValueWritable.class) {
          value = new ValueWritable(value);
        }
      }
    }
    try {
      context.write(key, value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Collect a key, value pair for later combining.
   *
   * @param key   The key to collect.
   * @param value The value to collect.
   */
  private void collect(Object key, Object value) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Collecting %s,%s for later combining", key.toString(), value.toString()));
    }
    List<Object> values = collector.get(key);
    if (values == null) {
      values = new LinkedList<Object>();
      collector.put(key, values);
    }
    values.add(value);
    numCollected++;

    if (numCollected > 104857) {
      runInMemoryCombiner();
      resetInMemoryCombiner();
    }
  }

  /**
   * Execute the in memory combiner on the collected key, value pairs.
   */
  void runInMemoryCombiner() {
    LOG.debug("Running the combiner on " + collector.size() + " keys");

    combining = true;
    for (Entry<Object, List<Object>> entry : collector.entrySet()) {
      try {
        combine.invoke(this, entry.getKey(), entry.getValue());
      } catch (Exception ex) {
      }
    }
  }

  /**
   * Set the {@link Delegator} class that will return BatyrJob to delegate to
   * for calls to setup(), map(), reduce(), combine(), or cleanup().
   *
   * @param job   The context for the job.
   * @param clazz The Delegator class.
   */
  static void setDelegatorClass(JobContext job, Class<? extends Delegator> clazz) {
    job.getConfiguration().setClass(DELEGATOR_CLASS_KEY, clazz, Delegator.class);
  }

  /**
   * Get an instance of the Delegator object for the given job.
   *
   * @param job The job to get the Delegator for.
   * @return The Delegator
   */
  static Delegator getDelegator(JobContext job) {
    return getDelegator(job.getConfiguration());
  }

  /**
   * Get an instance of the Delegator object from the given configuration.
   *
   * @param conf  The configuration object that has the Delegator set.
   * @return The Delegator
   */
  static Delegator getDelegator(Configuration conf) {
    Class<? extends Delegator> clazz = conf.getClass(DELEGATOR_CLASS_KEY, null, Delegator.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BatyrJob getJob() {
    return this;
  }

  /**
   * Set the task context for the currently executing task of this job. This
   * method enables the generic write() method to output the results of both
   * map tasks and reduce tasks.
   *
   * @param context The context for the current task.
   */
  void setContext(TaskInputOutputContext context) {
    this.context = context;
  }
  // </editor-fold>

  // <editor-fold defaultstate="collapsed" desc="Methods from Job">
  /**
   * {@inheritDoc}
   */
  public void failTask(TaskAttemptID taskId) throws IOException {
    job.failTask(taskId);
  }

  /**
   * {@inheritDoc}
   */
  public Counters getCounters() throws IOException {
    return job.getCounters();
  }

  /**
   * {@inheritDoc}
   */
  public String getJar() {
    return job.getJar();
  }

  /**
   * {@inheritDoc}
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) throws IOException {
    return job.getTaskCompletionEvents(startFrom);
  }

  /**
   * {@inheritDoc}
   */
  public String getTrackingURL() {
    return job.getTrackingURL();
  }

  /**
   * {@inheritDoc}
   */
  public boolean isComplete() throws IOException {
    return job.isComplete();
  }

  /**
   * {@inheritDoc}
   */
  public boolean isSuccessful() throws IOException {
    return job.isSuccessful();
  }

  /**
   * {@inheritDoc}
   */
  public void killJob() throws IOException {
    job.killJob();
  }

  /**
   * {@inheritDoc}
   */
  public void killTask(TaskAttemptID taskId) throws IOException {
    job.killTask(taskId);
  }

  /**
   * {@inheritDoc}
   */
  public float mapProgress() throws IOException {
    return job.mapProgress();
  }

  /**
   * {@inheritDoc}
   */
  public float reduceProgress() throws IOException {
    return job.reduceProgress();
  }

  /**
   * {@inheritDoc}
   */
  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    job.setCancelDelegationTokenUponJobCompletion(value);
  }

  /**
   * {@inheritDoc}
   */
  public void setCombinerClass(Class<? extends Reducer> cls) throws IllegalStateException {
    job.setCombinerClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
    job.setGroupingComparatorClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setInputFormatClass(Class<? extends InputFormat> cls) throws IllegalStateException {
    job.setInputFormatClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setJarByClass(Class<?> cls) {
    job.setJarByClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setJobName(String name) throws IllegalStateException {
    job.setJobName(name);
  }

  /**
   * {@inheritDoc}
   */
  public void setMapOutputKeyClass(Class<?> theClass) throws IllegalStateException {
    job.setMapOutputKeyClass(theClass);
  }

  /**
   * {@inheritDoc}
   */
  public void setMapOutputValueClass(Class<?> theClass) throws IllegalStateException {
    job.setMapOutputValueClass(theClass);
  }

  /**
   * {@inheritDoc}
   */
  public void setMapperClass(Class<? extends Mapper> cls) throws IllegalStateException {
    job.setMapperClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    job.setNumReduceTasks(tasks);
  }

  /**
   * {@inheritDoc}
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls) throws IllegalStateException {
    job.setOutputFormatClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setOutputKeyClass(Class<?> theClass) throws IllegalStateException {
    job.setOutputKeyClass(theClass);
  }

  /**
   * {@inheritDoc}
   */
  public void setOutputValueClass(Class<?> theClass) throws IllegalStateException {
    job.setOutputValueClass(theClass);
  }

  /**
   * {@inheritDoc}
   */
  public void setPartitionerClass(Class<? extends Partitioner> cls) throws IllegalStateException {
    job.setPartitionerClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setReducerClass(Class<? extends Reducer> cls) throws IllegalStateException {
    job.setReducerClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setSortComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
    job.setSortComparatorClass(cls);
  }

  /**
   * {@inheritDoc}
   */
  public void setUserClassesTakesPrecedence(boolean value) {
    job.setUserClassesTakesPrecedence(value);
  }

  /**
   * {@inheritDoc}
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    job.setWorkingDirectory(dir);
  }

  /**
   * {@inheritDoc}
   */
  public float setupProgress() throws IOException {
    return job.setupProgress();
  }

  /**
   * {@inheritDoc}
   */
  public void submit() throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
  }

  /**
   * {@inheritDoc}
   */
  public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
    return job.waitForCompletion(verbose);
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
    return job.getCombinerClass();
  }

  /**
   * {@inheritDoc}
   */
  public Configuration getConfiguration() {
    return job.getConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  public Credentials getCredentials() {
    return job.getCredentials();
  }

  /**
   * {@inheritDoc}
   */
  public RawComparator<?> getGroupingComparator() {
    return job.getGroupingComparator();
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
    return job.getInputFormatClass();
  }

  /**
   * {@inheritDoc}
   */
  public JobID getJobID() {
    return job.getJobID();
  }

  /**
   * {@inheritDoc}
   */
  public String getJobName() {
    return job.getJobName();
  }

  /**
   * {@inheritDoc}
   */
  public Class<?> getMapOutputKeyClass() {
    return job.getMapOutputKeyClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<?> getMapOutputValueClass() {
    return job.getMapOutputValueClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
    return job.getMapperClass();
  }

  /**
   * {@inheritDoc}
   */
  public int getNumReduceTasks() {
    return job.getNumReduceTasks();
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
    return job.getOutputFormatClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<?> getOutputKeyClass() {
    return job.getOutputKeyClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<?> getOutputValueClass() {
    return job.getOutputValueClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
    return job.getPartitionerClass();
  }

  /**
   * {@inheritDoc}
   */
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
    return job.getReducerClass();
  }

  /**
   * {@inheritDoc}
   */
  public RawComparator<?> getSortComparator() {
    return job.getSortComparator();
  }

  /**
   * {@inheritDoc}
   */
  public Path getWorkingDirectory() throws IOException {
    return job.getWorkingDirectory();
  }

  /**
   * {@inheritDoc}
   */
  public boolean userClassesTakesPrecedence() {
    return job.userClassesTakesPrecedence();
  }
  //</editor-fold>
}
