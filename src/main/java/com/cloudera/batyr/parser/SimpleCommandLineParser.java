package com.cloudera.batyr.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class SimpleCommandLineParser {
  
  private static Logger LOG = Logger.getLogger(SimpleCommandLineParser.class);
  
  public static String[] parse(String[] args, String[] options, String[] flags, Map<String, String> settings) {
    return parse(args, new HashSet<String>(Arrays.asList(options)), new HashSet<String>(Arrays.asList(flags)), settings);
  }
  
  public static String[] parse(String[] args, Set<String> options, Set<String> flags, Map<String, String> settings) {
    List<String> extraArgs = new LinkedList<String>();
    
    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].startsWith("-")) {
        String opt = args[idx].replaceFirst("-*", "");
        if (options.contains(opt)) {
          settings.put(opt, args[++idx]);
        } else if (flags.contains(opt)) {
          settings.put(opt, Boolean.toString(true));
        } else {
          extraArgs.add(args[idx]);
        }
      } else {
        extraArgs.add(args[idx]);
      }
    }
    
    return extraArgs.toArray(new String[extraArgs.size()]);
  }
}
