package com.cloudera.batyr.reflect;

import com.cloudera.batyr.mapreduce.BatyrJob;
import java.lang.reflect.Method;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class MethodGrabberTest {

  @Test
  public void testGetMap() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("map", String.class, String.class);
    Method result = MethodGrabber.getMap(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMapNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getMap(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMapCleanup() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("cleanup", Mapper.Context.class);
    Method result = MethodGrabber.getMapCleanup(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetMapCleanupNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getMapCleanup(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGetPartition() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("getPartition", String.class, String.class, int.class);
    Method result = MethodGrabber.getGetPartition(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGetPartitionNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getGetPartition(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReduce() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("reduce", String.class, Iterable.class);
    Method result = MethodGrabber.getReduce(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReduceNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getReduce(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetCombine() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("combine", String.class, Iterable.class);
    Method result = MethodGrabber.getCombine(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetCombineNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getCombine(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReduceCleanup() throws Exception {
    BatyrJob job = new TestJob();
    Method expResult = TestJob.class.getDeclaredMethod("cleanup", Reducer.Context.class);
    Method result = MethodGrabber.getReduceCleanup(job);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReduceCleanupNull() throws Exception {
    BatyrJob job = new EmptyJob();
    Method expResult = null;
    Method result = MethodGrabber.getReduceCleanup(job);
    assertEquals(expResult, result);
  }

}
