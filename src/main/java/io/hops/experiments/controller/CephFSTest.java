/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.controller;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author salman
 */
public class CephFSTest {
 public static void main(String[] argv) throws IOException {
    new CephFSTest().start();
  }

  private void start() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.ceph.impl", "org.apache.hadoop.fs.ceph.CephFileSystem");
    conf.set("fs.default.name", "ceph:///");
    conf.set("ceph.conf.file", "/etc/ceph/ceph.conf");
    conf.set("ceph.root.dir", "/");
    conf.set("ceph.mon.address", "salman2:6789");
    conf.set("ceph.auth.id", "admin");
    conf.set("ceph.auth.keyring", "/etc/ceph/ceph.client.admin.keyring");
    
    
    System.out.println("Initializing ... ");
     FileSystem client = (FileSystem) FileSystem.newInstance(conf);

     System.out.println("Initialized.");
     
     client.mkdirs(new Path("/test1"));
     client.mkdirs(new Path("/test2"));
     client.mkdirs(new Path("/test3"));
     
     
     client.create(new Path("/file.txt")).close();
     
     
     
     FileStatus[] files = client.listStatus(new Path("/"));
     for(FileStatus file : files){
       System.out.println("Files "+file);
     }
     
     System.out.println("Terminating.");
     System.exit(0);
  }
}
