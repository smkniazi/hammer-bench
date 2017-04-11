/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks;

import io.hops.experiments.controller.ConfigKeys;
import io.hops.experiments.controller.Configuration;
import io.hops.experiments.utils.BenchmarkUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author salman
 */
public class MoveThenDelete {

  private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
  private Configuration args;

  public static void main(String[] argv) throws Exception {
    
    String configFilePath = "master.properties";
    if (argv.length != 1) {
      System.out.println("Enter the folder name to rename  and then delete ");
      return;
    }
    new MoveThenDelete().start("master.properties", argv[0]);
  }

  private void start(String configFilePath, String hdfsFolder)  {
    try {
      args = new Configuration(configFilePath);
      org.apache.hadoop.conf.Configuration conf = createHdfsConf();

      // do shit here
      FileSystem dfs = BenchmarkUtils.getDFSClient(conf);
      

      Path from = new Path(hdfsFolder);
      Path to = new Path(hdfsFolder+"1");
      
      Long startTime = System.currentTimeMillis();
      dfs.rename(from, to);
      Long endTime = System.currentTimeMillis();

      System.out.println("Move time taken in ms " + (endTime - startTime));
      System.out.println("Move time taken in minutes " + (endTime - startTime) / (double) (1000 * 60));
      
      
      startTime = System.currentTimeMillis();
      dfs.delete(to,true);
      endTime = System.currentTimeMillis();

      System.out.println("Delete time taken in ms " + (endTime - startTime));
      System.out.println("Delete time taken in minutes " + (endTime - startTime) / (double) (1000 * 60));
      
    } catch(Throwable e){
      System.out.println(e);
    }
    finally {
      System.out.println("Exiting ... ");
      System.exit(0);
    }
  }

  private org.apache.hadoop.conf.Configuration createHdfsConf() {
    org.apache.hadoop.conf.Configuration dfsClientConf = new org.apache.hadoop.conf.Configuration();
    dfsClientConf.set(ConfigKeys.FS_DEFAULTFS_KEY, args.getNameNodeRpcAddress());
    dfsClientConf.set(ConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY, Long.toString(args.getNameNodeRefreshRate()));
    dfsClientConf.set(ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, args.getNameNodeSelectorPolicy());
    return dfsClientConf;
  }
}
