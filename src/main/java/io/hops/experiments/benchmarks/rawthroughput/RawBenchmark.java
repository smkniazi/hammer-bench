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
package io.hops.experiments.benchmarks.rawthroughput;

import io.hops.experiments.benchmarks.OperationsUtils;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.commands.NamespaceWarmUp;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.WarmUpCommand;
import org.apache.hadoop.conf.Configuration;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author salman
 */
public class RawBenchmark extends Benchmark {

  private AtomicInteger successfulOps = new AtomicInteger(0);
  private AtomicInteger failedOps = new AtomicInteger(0);
  private long phaseStartTime;
  private long phaseDurationInMS;
  private final long maxFilesToCreate;
  private String baseDir;
  private short replicationFactor;
  private long fileSize;
  private long appendSize;
  private final int dirsPerDir;
  private final int filesPerDir;
  private final int treeDepth;
  private final boolean fixedDepthTree;
  
  public RawBenchmark(Configuration conf, int numThreads, int dirsPerDir, 
          int filesPerDir, long maxFilesToCreate,
          boolean fixedDepthTree, int treeDepth, BenchMarkFileSystemName fsName) {
    super(conf, numThreads, fsName);
    this.dirsPerDir = dirsPerDir;
    this.filesPerDir = filesPerDir;
    this.maxFilesToCreate = maxFilesToCreate;
    this.fixedDepthTree = fixedDepthTree;
    this.treeDepth = treeDepth;
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand)
          throws IOException, InterruptedException {
    NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
    this.replicationFactor = namespaceWarmUp.getReplicationFactor();
    this.fileSize = namespaceWarmUp.getFileSize();
    this.appendSize = namespaceWarmUp.getAppendSize();
    this.baseDir = namespaceWarmUp.getBaseDir();
    List workers = new ArrayList<BaseWarmUp>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new BaseWarmUp(namespaceWarmUp.getFilesToCreate(), replicationFactor,
              fileSize, baseDir, dirsPerDir, filesPerDir, fixedDepthTree, treeDepth);
      workers.add(worker);
    }
    executor.invokeAll(workers);
    Logger.printMsg("Completed Warmup Phase");
    workers.clear();
    return new NamespaceWarmUp.Response();
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command)
          throws IOException, InterruptedException {
    RawBenchmarkCommand.Request request = (RawBenchmarkCommand.Request) command;
    RawBenchmarkCommand.Response response;
    System.out.println("Starting the " + request.getPhase() + " duration " + request.getDurationInMS());
    response = startTestPhase(request.getPhase(), request.getDurationInMS(), baseDir);
    return response;
  }

  private RawBenchmarkCommand.Response startTestPhase(BenchmarkOperations opType, long duration, String baseDir) throws InterruptedException, UnknownHostException, IOException {
    List workers = new LinkedList<Callable>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new Generic(baseDir, opType);
      workers.add(worker);
    }
    setMeasurementVariables(duration);

    Logger.resetTimer();

    executor.invokeAll(workers);// blocking call
    long phaseFinishTime = System.currentTimeMillis();
    long actualExecutionTime = (phaseFinishTime - phaseStartTime);
    
    double speed = ((double) successfulOps.get() / (double) actualExecutionTime); // p / ms
    speed = speed * 1000;

    RawBenchmarkCommand.Response response =
            new RawBenchmarkCommand.Response(opType,
            actualExecutionTime, successfulOps.get(), failedOps.get(), speed, getAliveNNsCount());
    return response;
  }

  public class Generic implements Callable {

    private BenchmarkOperations opType;
    private FileSystem dfs;
    private FilePool filePool;
    private String baseDir;

    public Generic(String baseDir, BenchmarkOperations opType) throws IOException {
      this.baseDir = baseDir;
      this.opType = opType;
    }

    @Override
    public Object call() throws Exception {
      try{
        dfs = BenchmarkUtils.getDFSClient(conf);
        filePool = BenchmarkUtils.getFilePool(conf, baseDir, 
              dirsPerDir, filesPerDir, fixedDepthTree, treeDepth);
      }catch(Exception e){
        Logger.error(e);
        e.printStackTrace();
        throw e;
      }
      while (true) {
        try {

          String path = OperationsUtils.getPath(opType,filePool);

          if (path == null 
                  || ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS))
                  || (opType == BenchmarkOperations.CREATE_FILE && 
                      maxFilesToCreate < (long)(successfulOps.get() 
                                         + filesCreatedInWarmupPhase.get()))) {
            return null;
          }
          
          OperationsUtils.performOp(dfs,opType,filePool,path,replicationFactor,fileSize, appendSize);
          
          successfulOps.incrementAndGet();

          if (Logger.canILog()) {
            Logger.printMsg("Successful " + opType + " ops " + successfulOps.get() + " Failed ops " + failedOps.get() + " Speed: " + BenchmarkUtils.round(speedPSec(successfulOps, phaseStartTime)));
          }
        } catch (Exception e) {
          failedOps.incrementAndGet();
          Logger.error(e);
        }
      }
    }
  }

  private void setMeasurementVariables(long duration) {
    phaseDurationInMS = duration;
    phaseStartTime = System.currentTimeMillis();
    successfulOps = new AtomicInteger(0);
    failedOps = new AtomicInteger(0);
  }

  public double speedPSec(AtomicInteger ops, long startTime) {
    long timePassed = (System.currentTimeMillis() - startTime);
    double opsPerMSec = (double) (ops.get()) / (double) timePassed;
    return opsPerMSec * 1000;
  }
}
