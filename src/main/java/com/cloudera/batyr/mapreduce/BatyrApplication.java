package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.parser.SimpleCommandLineParser;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BatyrApplication implements Tool, Delegator {

  private static Logger LOG = Logger.getLogger(BatyrApplication.class);
  private static final String PHASE_KEY = BatyrApplication.class.getName() + ".phase";
  private List<Do> phases = new ArrayList<Do>();
  private List<Method> maps = new ArrayList<Method>();
  private List<Method> getPartitions = new ArrayList<Method>();
  private List<Method> reduces = new ArrayList<Method>();
  private Configuration conf;
  private Random random;

  public BatyrApplication() {
    this(new Configuration());
  }

  public BatyrApplication(Configuration conf) {
    this.conf = conf;
    random = new Random();
  }

  public void phase(Do phase) {
    phases.add(phase);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Map<String, String> settings = new HashMap<String, String>();
    args = SimpleCommandLineParser.parse(args, new String[]{"output"}, new String[0], settings);
    Path finalOutputPath = new Path(settings.get("output"));
    String outputPrefix = String.format("%s_%010d_", finalOutputPath.getName(), random.nextInt() & Integer.MAX_VALUE);

    for (int idx = 0; idx < phases.size(); idx++) {
      Do phase = phases.get(idx);
      args = phase.configureJob(args);
      if (idx > 0) {
        phase.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(phase.job, new Path(finalOutputPath.getParent(), String.format("%s%05d", outputPrefix, idx - 1)));
      }

      if (idx < (phases.size() - 1)) {
        phase.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(phase.job, new Path(finalOutputPath.getParent(), String.format("%s%05d", outputPrefix, idx)));
      } else {
        FileOutputFormat.setOutputPath(phase.job, finalOutputPath);
      }
      phase.job.getConfiguration().setInt(PHASE_KEY, idx);
      BatyrJob.setDelegatorClass(phase.job, getClass());

      if (!phase.waitForCompletion(true)) {
        LOG.error("The job for phase " + idx + " failed, see JobTracker for details");
        return 1;
      }
    }
    return 0;
  }

  public void map(Object key, Object value, int phaseIdx) {
    Do phase = phases.get(phaseIdx);
    Method map = null;
    Class<?> keyClass;
    Class<?> valueClass;
    for (Method method : phase.getClass().getDeclaredMethods()) {
      if (method.getName().equals("map") && method.getParameterTypes().length == 2) {
        map = method;
        keyClass = map.getParameterTypes()[0];
        valueClass = map.getParameterTypes()[1];
        break;
      }
    }
    map.setAccessible(true);
    try {
      map.invoke(phase, key, value);
    } catch (Exception ex) {
      LOG.error("Call to map() failed", ex);
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) {
    String applicationClassName = getApplicationClassName();
    try {
      Class<? extends BatyrApplication> applicationClass = Class.forName(applicationClassName).asSubclass(BatyrApplication.class);
      BatyrApplication job = applicationClass.newInstance();
      System.exit(ToolRunner.run(job, args));
    } catch (ClassNotFoundException ex) {
      LOG.error("Can't find your job class (" + applicationClassName + "), how did you execute main()?", ex);
    } catch (IllegalAccessException ex) {
      LOG.error("No public noargs constructor for your job class (" + applicationClassName + ")", ex);
    } catch (InstantiationException ex) {
      LOG.error("Can't instantiate your job class (" + applicationClassName + ")", ex);
    } catch (Exception ex) {
      LOG.error("Error trying to run your job", ex);
    }
  }

  private static String getApplicationClassName() {
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
  public BatyrJob getJob() {
    int phaseIdx = conf.getInt(PHASE_KEY, 0);
    return phases.get(phaseIdx);
  }
}
