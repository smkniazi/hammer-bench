/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateRandomFile {

  static int numFiles;
  static int numThreads;

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new RuntimeException("Wrong number of arguments. Expected two arguments. arg1: number of files, arg2 number of threads");
      
    }
    else{
      numFiles = Integer.parseInt(args[0]);
      numThreads = Integer.parseInt(args[1]);
      System.out.println("Creating "+numFiles+" files using "+numThreads+" threads");
    }
    
    
    CreateRandomFile f = new CreateRandomFile();
    f.startWriting();
    System.exit(0);
  }
  
  public void startWriting() throws InterruptedException{
    
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numFiles; i++) {
      Runnable worker = new Writer();
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      Thread.sleep(100);
    }
  }

  public class Writer implements Runnable {

    private UUID fileName;

    public Writer() {
      fileName = UUID.randomUUID();
    }

    @Override
    public void run() {
      try{
    System.out.println("Starting to write "+fileName.toString());
    Configuration conf = new Configuration();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    long startTime = System.currentTimeMillis();
    Path path = new Path("/"+fileName.toString()+"/"+fileName.toString());
    BenchmarkUtils.createFile(dfs, path, (short) 1, 1);
    System.out.println("Finished file "+fileName.toString()+". Time taken : " + (System.currentTimeMillis()-startTime)/1000 + " sec");
      }catch(Exception e){
        e.printStackTrace();
      }
    }
  }
}
