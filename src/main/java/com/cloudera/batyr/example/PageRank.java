package com.cloudera.batyr.example;

import com.cloudera.batyr.mapreduce.BatyrJob;
import java.io.IOException;
import org.apache.log4j.Logger;

public class PageRank extends BatyrJob {

  private static Logger LOG = Logger.getLogger(PageRank.class);

  public void map(long offset, String line) throws IOException, InterruptedException {
    // A,1.0\tB C D
    String[] source_outboundLinks = line.split("\t", 2);
    Vertex source = new Vertex(source_outboundLinks[0]);
    String[] outboundLinks = source_outboundLinks[1].split(" ");
    if (!outboundLinks[0].isEmpty()) {
      double love = source.getPagerank() / outboundLinks.length;
      for (String destination : outboundLinks) {
        write(Long.parseLong(destination), "l:" + love);
      }
    } else {
      // If I have no outbound links, send all of my love to me!
      write(source.getId(), "l:" + source.getPagerank());
    }
    write(source.getId(), "d:" + source_outboundLinks[1]);
  }
  double damping = 0.85;

  public void reduce(long id, Iterable<String> links) throws IOException, InterruptedException {
    String outbound = null;
    double pageRank = 1 - damping;
    double totalLove = 0;
    for (String link : links) {
      if (link.startsWith("l")) {
        // Some love
        totalLove += Double.parseDouble(link.split(":")[1]);
      } else {
        // My outbound links
        outbound = (link.endsWith(":") ? "" : link.split(":")[1]);
      }
    }
    if (outbound == null) {
      LOG.error("No outbound links sent to vertex "+id);
    }
    pageRank += (damping * totalLove);
    Vertex source = new Vertex(id, pageRank);
    write(source.toString(), outbound);
  }
}
