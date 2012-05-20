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
    return parse(args, options, flags, settings, true);
  }

  public static String[] parse(String[] args, String[] options, String[] flags, Map<String, String> settings, boolean consume) {
    return parse(args, new HashSet<String>(Arrays.asList(options)), new HashSet<String>(Arrays.asList(flags)), settings, consume);
  }

  public static String[] parse(String[] args, Set<String> options, Set<String> flags, Map<String, String> settings) {
    return parse(args, options, flags, settings, true);
  }

  public static String[] parse(String[] args, Set<String> options, Set<String> flags, Map<String, String> settings, boolean consume) {
    List<String> extraArgs = new LinkedList<String>();

    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].startsWith("-")) {
        String opt = args[idx].replaceFirst("-*", "");
        if (options.contains(opt)) {
          if (!consume) {
            extraArgs.add(args[idx]);
            extraArgs.add(args[idx + 1]);
          }
          settings.put(opt, args[++idx]);
        } else if (flags.contains(opt)) {
          if (!consume) {
            extraArgs.add(args[idx]);
          }
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
