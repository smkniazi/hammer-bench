/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.controller;


import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.utils.DFSOperationsUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author salman
 */
public class HopsFSConnecttionTest {

//  private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//  private BMConfiguration args;
//
//  private static int length = 1024;
//  public static void main(String[] argv) throws Exception {
//    if(argv.length == 1){
//      try{
//        length = Integer.parseInt(argv[0]);
//      } catch (Exception e) {
//      }
//    }
//    System.out.println("RandData length is "+length);
//    new HopsFSConnecttionTest().start2();
//  }
//
//  private void start2() throws IOException {
//
//    Logger.getRootLogger().setLevel(Level.WARN);
//    Configuration conf = new Configuration();
//    conf.set("fs.defaultFS", "hdfs://rpc.namenode.service.consul:8020");
//    conf.setBoolean("ipc.server.ssl.enabled", true);
//    conf.set("hadoop.ssl.hostname.verifier", "ALLOW_ALL");
//    conf.set("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.HopsSSLSocketFactory");
//    conf.set("hadoop.ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1,SSLv3");
//    conf.set("client.rpc.ssl.enabled.protocol", "TLSv1.2");
//    conf.set("fs.defaultFS", "hdfs://rpc.namenode.service.consul:8020");
//    conf.set("hops.tls.superuser-material-directory", "/srv/hops/super_crypto/${USER}");
//    conf.set("client.materialize.directory", "/srv/hops/certs-dir/transient");
//    conf.set("hadoop.proxyuser.hdfs.hosts", "*");
//    conf.set("hadoop.proxyuser.hdfs.groups", "*");
//
//
//    FileSystem dfs = FileSystem.newInstance(conf);
//
//   // FileStatus[] listing = dfs.listStatus(new Path("/"));
//
//
//
//   // for (FileStatus status : listing) {
//   //   System.out.println(status.getPath());
//   // }
// 
//
//   note: 10009 is the id ofthe dataset `testproj`
//
//    String randData = new RandomString(length).nextString();
//
//    String value = "{\"prov_type\":{\"meta_status\":\"FULL_PROV_ENABLED\"," +
//            "\"prov_status\":\"ALL\"}," +
//            "\"project_iid\":10009," +
//            "\"dummy_data\": \""+randData+"\"}";
//    dfs.setXAttr(new Path("/Projects/testproj/Experiments"), "provenance.core", value.getBytes());
//
//    System.exit(0);
//
//  }
//
//  private void start() {
//    try {
//      System.out.println("Starting...");
//      org.apache.hadoop.conf.Configuration conf = createHdfsConfTest();
//
//      // do shit here
//      System.out.println("FS URI is : " + conf.get("fs.defaultFS"));
//
//      Path pt = new Path("."); // HDFS Path
////    FileSystem dfs = pt.getFileSystem(conf);
//      FileSystem dfs = FileSystem.newInstance(conf);
//
//      FileStatus[] listing = dfs.listStatus(new Path("/"));
//
//      for (FileStatus status : listing) {
//        System.out.println(status.getPath());
//      }
//
//
//    } catch (Throwable e) {
//      e.printStackTrace();
//    } finally {
//      System.out.println("Exiting ... ");
//      System.exit(0);
//    }
//  }
//
//  private org.apache.hadoop.conf.Configuration createHdfsConfTest() throws IOException {
//
//    Configuration conf = new Configuration();
//    conf.addResource(new Path("file:///srv/hops/hadoop/etc/hadoop/core-site.xml")); // Replace with actual path
//    conf.addResource(new Path("file:///srv/hops/hadoop/etc/hadoop/hdfs-site.xml"));
//
////    conf.addResource(new Path("file:///tmp/core-site.xml")); // Replace with actual path
////    conf.addResource(new Path("file:///tmp/hdfs-site.xml"));
//    return conf;
//  }
//
//
//
//  static class RandomString {
//    /**
//     * Generate a random string.
//     */
//    public String nextString() {
//      for (int idx = 0; idx < buf.length; ++idx)
//        buf[idx] = symbols[random.nextInt(symbols.length)];
//      return new String(buf);
//    }
//
//    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
//
//    public static final String lower = upper.toLowerCase(Locale.ROOT);
//
//    public static final String digits = "0123456789";
//
//    public static final String alphanum = upper + lower + digits;
//
//    private final Random random;
//
//    private final char[] symbols;
//
//    private final char[] buf;
//
//    public RandomString(int length, Random random, String symbols) {
//      if (length < 1) throw new IllegalArgumentException();
//      if (symbols.length() < 2) throw new IllegalArgumentException();
//      this.random = new Random(0);
//      this.symbols = symbols.toCharArray();
//      this.buf = new char[length];
//    }
//
//    /**
//     * Create an alphanumeric string generator.
//     */
//    public RandomString(int length, Random random) {
//      this(length, random, alphanum);
//    }
//
//    /**
//     * Create an alphanumeric strings from a secure generator.
//     */
//    public RandomString(int length) {
//      this(length, new SecureRandom());
//    }
//
//    /**
//     * Create session identifiers.
//     */
//    public RandomString() {
//      this(21);
//    }
//
//  }
}
