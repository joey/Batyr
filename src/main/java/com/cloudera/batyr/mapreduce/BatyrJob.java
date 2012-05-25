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
  public final static String OUTPUT_CLASS_KEY = BatyrJob.class.getName() + ".output.format";
  protected Configuration conf;
  protected String jobName;
  protected Job job;
  private TaskInputOutputContext context;

  protected BatyrJob(Configuration conf, String jobName) {
    this.conf = conf;
    this.jobName = (jobName != null ? jobName : getClass().getSimpleName());
    try {
      job = new Job(this.conf, this.jobName);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected BatyrJob(Configuration conf) {
    this(conf, null);
  }

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
   * @throws Exception 
   */
  protected String[] configureManually(String[] args) throws Exception {
    return args;
  }

  String[] configureJob() throws Exception {
    return configureJob(new String[0]);
  }

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

  private String[] configureAutomatically(String[] args) throws Exception {
    setJarByClass(getClass());
    setDelegatorClass(job, getClass());
    args = setInput(args);
    args = setTasks(args);
    setNumReduceTasks(-1);
    args = setOutput(args);

    return args;
  }

  @SuppressWarnings("static-access")
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

  @SuppressWarnings("static-access")
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
        job.getConfiguration().setClass(OUTPUT_CLASS_KEY, Format.getOutputFormat(output), OutputFormat.class);
      } else {
        setOutputFormatClass(Format.getOutputFormat(output));
      }
    }

    return args;
  }

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

  void setInMemoryCombiner(Method combine) {
    this.combine = combine;
    collector = new TreeMap<Object, List<Object>>();
    resetInMemoryCombiner();
  }

  private void resetInMemoryCombiner() {
    collector.clear();
    numCollected = 0;
    combining = false;
  }

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

  static void setDelegatorClass(JobContext job, Class<? extends Delegator> clazz) {
    job.getConfiguration().setClass(DELEGATOR_CLASS_KEY, clazz, Delegator.class);
  }

  static Delegator getDelegator(JobContext job) {
    return getDelegator(job.getConfiguration());
  }

  static Delegator getDelegator(Configuration conf) {
    Class<? extends Delegator> clazz = conf.getClass(DELEGATOR_CLASS_KEY, null, Delegator.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  @Override
  public BatyrJob getJob() {
    return this;
  }

  void setContext(TaskInputOutputContext context) {
    this.context = context;
  }
  // </editor-fold>

  // <editor-fold defaultstate="collapsed" desc="Methods from Job">
  public void failTask(TaskAttemptID taskId) throws IOException {
    job.failTask(taskId);
  }

  public Counters getCounters() throws IOException {
    return job.getCounters();
  }

  public String getJar() {
    return job.getJar();
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) throws IOException {
    return job.getTaskCompletionEvents(startFrom);
  }

  public String getTrackingURL() {
    return job.getTrackingURL();
  }

  public boolean isComplete() throws IOException {
    return job.isComplete();
  }

  public boolean isSuccessful() throws IOException {
    return job.isSuccessful();
  }

  public void killJob() throws IOException {
    job.killJob();
  }

  public void killTask(TaskAttemptID taskId) throws IOException {
    job.killTask(taskId);
  }

  public float mapProgress() throws IOException {
    return job.mapProgress();
  }

  public float reduceProgress() throws IOException {
    return job.reduceProgress();
  }

  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    job.setCancelDelegationTokenUponJobCompletion(value);
  }

  public void setCombinerClass(Class<? extends Reducer> cls) throws IllegalStateException {
    job.setCombinerClass(cls);
  }

  public void setGroupingComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
    job.setGroupingComparatorClass(cls);
  }

  public void setInputFormatClass(Class<? extends InputFormat> cls) throws IllegalStateException {
    job.setInputFormatClass(cls);
  }

  public void setJarByClass(Class<?> cls) {
    job.setJarByClass(cls);
  }

  public void setJobName(String name) throws IllegalStateException {
    job.setJobName(name);
  }

  public void setMapOutputKeyClass(Class<?> theClass) throws IllegalStateException {
    job.setMapOutputKeyClass(theClass);
  }

  public void setMapOutputValueClass(Class<?> theClass) throws IllegalStateException {
    job.setMapOutputValueClass(theClass);
  }

  public void setMapperClass(Class<? extends Mapper> cls) throws IllegalStateException {
    job.setMapperClass(cls);
  }

  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    job.setNumReduceTasks(tasks);
  }

  public void setOutputFormatClass(Class<? extends OutputFormat> cls) throws IllegalStateException {
    job.setOutputFormatClass(cls);
  }

  public void setOutputKeyClass(Class<?> theClass) throws IllegalStateException {
    job.setOutputKeyClass(theClass);
  }

  public void setOutputValueClass(Class<?> theClass) throws IllegalStateException {
    job.setOutputValueClass(theClass);
  }

  public void setPartitionerClass(Class<? extends Partitioner> cls) throws IllegalStateException {
    job.setPartitionerClass(cls);
  }

  public void setReducerClass(Class<? extends Reducer> cls) throws IllegalStateException {
    job.setReducerClass(cls);
  }

  public void setSortComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
    job.setSortComparatorClass(cls);
  }

  public void setUserClassesTakesPrecedence(boolean value) {
    job.setUserClassesTakesPrecedence(value);
  }

  public void setWorkingDirectory(Path dir) throws IOException {
    job.setWorkingDirectory(dir);
  }

  public float setupProgress() throws IOException {
    return job.setupProgress();
  }

  public void submit() throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
  }

  public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
    return job.waitForCompletion(verbose);
  }

  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
    return job.getCombinerClass();
  }

  public Configuration getConfiguration() {
    return job.getConfiguration();
  }

  public Credentials getCredentials() {
    return job.getCredentials();
  }

  public RawComparator<?> getGroupingComparator() {
    return job.getGroupingComparator();
  }

  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
    return job.getInputFormatClass();
  }

  public JobID getJobID() {
    return job.getJobID();
  }

  public String getJobName() {
    return job.getJobName();
  }

  public Class<?> getMapOutputKeyClass() {
    return job.getMapOutputKeyClass();
  }

  public Class<?> getMapOutputValueClass() {
    return job.getMapOutputValueClass();
  }

  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
    return job.getMapperClass();
  }

  public int getNumReduceTasks() {
    return job.getNumReduceTasks();
  }

  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
    return job.getOutputFormatClass();
  }

  public Class<?> getOutputKeyClass() {
    return job.getOutputKeyClass();
  }

  public Class<?> getOutputValueClass() {
    return job.getOutputValueClass();
  }

  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
    return job.getPartitionerClass();
  }

  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
    return job.getReducerClass();
  }

  public RawComparator<?> getSortComparator() {
    return job.getSortComparator();
  }

  public Path getWorkingDirectory() throws IOException {
    return job.getWorkingDirectory();
  }

  public boolean userClassesTakesPrecedence() {
    return job.userClassesTakesPrecedence();
  }
  //</editor-fold>
}
