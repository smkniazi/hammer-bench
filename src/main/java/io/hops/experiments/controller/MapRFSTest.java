/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.controller;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author salman
 */
public class MapRFSTest {
  public static void main(String[] argv) throws IOException {
    new MapRFSTest().start();
  }

  private void start() throws IOException {
    Configuration conf = new Configuration();


    System.out.println("Initializing ... ");
    FileSystem client = (FileSystem) FileSystem.newInstance(URI.create("maprfs:///"), conf);

    System.out.println("Initialized.");


    client.create(new Path("/test/file.txt"), (short) 3).close();
    if (client.mkdirs(new Path("/test/test1"))) {
      System.out.println("Created dir");
    }
    FileStatus[] files = client.listStatus(new Path("/test"));
    for (FileStatus file : files) {
      System.out.println("Files " + file.getPath().toString());
    }

    System.out.println("Terminating.");
    System.exit(0);
  }
}
