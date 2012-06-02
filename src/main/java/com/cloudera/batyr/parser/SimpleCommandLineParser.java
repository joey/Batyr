package com.cloudera.batyr.parser;

import java.util.*;

public class SimpleCommandLineParser {

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

  Set<String> options;
  Set<String> flags;
  public SimpleCommandLineParser(String[] options, String[] flags) {
    this(new HashSet<String>(Arrays.asList(options)), new HashSet<String>(Arrays.asList(flags)));
  }

  public SimpleCommandLineParser(Set<String> options, Set<String> flags) {
    this.options = options;
    this.flags = flags;
  }

  public String[] parse(String[] args, Map<String, String> settings) {
    return parse(args, options, flags, settings);
  }

  public String[] parse(String[] args, Map<String, String> settings, boolean consume) {
    return parse(args, options, flags, settings, consume);
  }

}
