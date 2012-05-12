package com.cloudera.batyr.example;

import org.apache.log4j.Logger;

public class Vertex {
  
  private static Logger LOG = Logger.getLogger(Vertex.class);
  private long id;
  private double pagerank;

  public Vertex(long id, double  pagerank) {
    this.id = id;
    this.pagerank = pagerank;
  }
  
  public Vertex(String vertex) {
    String[] parts = vertex.split(",");
    this.id = Long.parseLong(parts[0]);
    this.pagerank = (parts.length == 2 ? Double.parseDouble(parts[1]) : 0.0);
  }

  public long getId() {
    return id;
  }

  public double getPagerank() {
    return pagerank;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setPagerank(double pagerank) {
    this.pagerank = pagerank;
  }

  @Override
  public String toString() {
    return id+","+pagerank;
  }
  
}
