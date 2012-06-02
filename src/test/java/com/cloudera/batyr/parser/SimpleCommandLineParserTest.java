package com.cloudera.batyr.parser;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SimpleCommandLineParserTest {

  @Test
  public void testParseWithConsumption() {
    String[] args = {"-input", "in_dir", "-output", "out_dir"};
    String[] options = {"input"};
    String[] flags = {};
    Map<String, String> settings = new HashMap<String, String>();
    String[] expResult = {"-output", "out_dir"};
    String[] result = SimpleCommandLineParser.parse(args, options, flags, settings);
    assertArrayEquals(expResult, result);
    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("input", "in_dir");
    assertEquals(expectedSettings, settings);
  }

  @Test
  public void testParseWithoutConsumption() {
    String[] args = {"-input", "in_dir", "-output", "out_dir"};
    String[] options = {"input", "output"};
    String[] flags = {};
    Map<String, String> settings = new HashMap<String, String>();
    String[] expResult = {"-input", "in_dir", "-output", "out_dir"};
    String[] result = SimpleCommandLineParser.parse(args, options, flags, settings, false);
    assertArrayEquals(expResult, result);
    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("input", "in_dir");
    expectedSettings.put("output", "out_dir");
    assertEquals(expectedSettings, settings);
  }

  @Test
  public void testParseFlagsWithConsumption() {
    String[] args = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    String[] options = {};
    String[] flags = {"recurse"};
    Map<String, String> settings = new HashMap<String, String>();
    String[] expResult = {"-input", "in_dir", "-output", "out_dir"};
    String[] result = SimpleCommandLineParser.parse(args, options, flags, settings);
    assertArrayEquals(expResult, result);
    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("recurse", "true");
    assertEquals(expectedSettings, settings);
  }

  @Test
  public void testParseFlagsWithOutConsumption() {
    String[] args = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    String[] options = {};
    String[] flags = {"recurse"};
    Map<String, String> settings = new HashMap<String, String>();
    String[] expResult = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    String[] result = SimpleCommandLineParser.parse(args, options, flags, settings, false);
    assertArrayEquals(expResult, result);
    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("recurse", "true");
    assertEquals(expectedSettings, settings);
  }

  @Test
  public void testNonStaticParseOptionsAndFlagsWithConsumption() {
    String[] options = {"input"};
    String[] flags = {"recurse"};
    SimpleCommandLineParser parser = new SimpleCommandLineParser(options, flags);

    String[] args = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    Map<String, String> settings = new HashMap<String, String>();

    String[] expResult = {"-output", "out_dir"};
    String[] result = parser.parse(args, settings);
    assertArrayEquals(expResult, result);

    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("input", "in_dir");
    expectedSettings.put("recurse", "true");
    assertEquals(expectedSettings, settings);
  }

  @Test
  public void testNonStaticParseOptionsAndFlagsWithOutConsumption() {
    String[] options = {"input"};
    String[] flags = {"recurse"};
    SimpleCommandLineParser parser = new SimpleCommandLineParser(options, flags);

    String[] args = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    Map<String, String> settings = new HashMap<String, String>();

    String[] expResult = {"-input", "in_dir", "-output", "out_dir", "-recurse"};
    String[] result = parser.parse(args, settings, false);
    assertArrayEquals(expResult, result);

    Map<String, String> expectedSettings = new HashMap<String, String>();
    expectedSettings.put("input", "in_dir");
    expectedSettings.put("recurse", "true");
    assertEquals(expectedSettings, settings);
  }

}
