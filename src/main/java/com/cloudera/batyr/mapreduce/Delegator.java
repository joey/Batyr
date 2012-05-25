package com.cloudera.batyr.mapreduce;

public interface Delegator {

  /**
   * Get the instance of BatyrJob to delegate to.
   * 
   * @return The instance of BatyrJob to delegate to.
   */
  public BatyrJob getJob();
}
