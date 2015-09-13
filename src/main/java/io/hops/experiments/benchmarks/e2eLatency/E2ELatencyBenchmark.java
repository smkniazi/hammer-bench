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
package io.hops.experiments.benchmarks.e2eLatency;

import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.workload.generator.FilePool;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 *
 * @author salman
 */
public class E2ELatencyBenchmark extends Benchmark {

  private long duration;
  private long startTime = 0;
  private String baseDir;
  private short replicationFactor;
  private long fileSize;
  private long appendSize;
    private final int dirsPerDir;
  private final int filesPerDir; 
  
  public E2ELatencyBenchmark(Configuration conf, int numThreads, int dirsPerDir, int filesPerDir) {
    super(conf, numThreads);
    this.dirsPerDir = dirsPerDir;
    this.filesPerDir = filesPerDir;
  }
  
  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand) throws IOException, InterruptedException {
    NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
    this.replicationFactor = namespaceWarmUp.getReplicationFactor();
    this.fileSize = namespaceWarmUp.getFileSize();
    this.appendSize = namespaceWarmUp.getAppendSize();
    this.baseDir = namespaceWarmUp.getBaseDir();
    List workers = new ArrayList<BaseWarmUp>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new BaseWarmUp(namespaceWarmUp.getFilesToCreate(), replicationFactor,
              fileSize, baseDir, dirsPerDir, filesPerDir);
      workers.add(worker);
    }
    executor.invokeAll(workers); // blocking call
    return new NamespaceWarmUp.Response();//To change body of generated methods, choose Tools | Templates.
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws IOException, InterruptedException {
     E2ELatencyBenchmarkCommand.Request request = (E2ELatencyBenchmarkCommand.Request) command;
    E2ELatencyBenchmarkCommand.Response response;
    System.out.println("Starting the end to end latency test. Duration " + request.getDurationInMS());
    response = startTestPhase(request.getDurationInMS(), baseDir);
    return response;
  }
  
  private E2ELatencyBenchmarkCommand.Response startTestPhase(long duration, String baseDir) throws InterruptedException, UnknownHostException, IOException {
    List workers = new LinkedList<Callable>();
    this.duration = duration;
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new E2EWorker(baseDir);
      workers.add(worker);
    }
    startTime = System.currentTimeMillis();
    executor.invokeAll(workers);// blocking call
    long finishTime = System.currentTimeMillis();
    long actualExecutionTime = (finishTime - startTime);
    
//    double speed = ((double) successfulOps.get() / (double) actualExecutionTime); // p / ms
//    speed = speed * 1000;

    E2ELatencyBenchmarkCommand.Response response =
            new E2ELatencyBenchmarkCommand.Response(actualExecutionTime);
    return response;
  }
  
  public class E2EWorker implements Callable {
    private DistributedFileSystem dfs;
    private FilePool filePool;
    private String baseDir;

    public E2EWorker(String baseDir) throws IOException {
      this.baseDir = baseDir;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, baseDir, dirsPerDir, filesPerDir);

      while (true) {
        try {

          //String path = OperationsUtils.getPath(opType,filePool);

          if (/*path == null ||*/ ((System.currentTimeMillis() - startTime) > (duration))) {
            return null;
          }
          
          //OperationsUtils.performOp(dfs,opType,filePool,path,replicationFactor,fileSize, appendSize);

          //successfulOps.incrementAndGet();

          if (Logger.canILog()) {
            Logger.printMsg("doing nothing ");
          }
        } catch (Exception e) {
          //failedOps.incrementAndGet();
          Logger.error(e);
        }
      }
    }
  }
}
