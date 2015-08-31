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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.WarmUpCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.workload.generator.FilePool;

/**
 *
 * @author salman
 */
public class RawBenchmark extends Benchmark {

  private AtomicInteger successfulOps = new AtomicInteger(0);
  private AtomicInteger failedOps = new AtomicInteger(0);
  private long phaseStartTime;
  private long phaseDurationInMS;
  private int maxFilesToCreate = Integer.MAX_VALUE;
  private String baseDir;
  private short replicationFactor;
  private long fileSize;
  //-- other
  private ExecutorService executor;

  public RawBenchmark(Configuration conf, int numThreads) {
    super(conf, numThreads);
    executor = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand)
          throws IOException, InterruptedException {
    NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
    this.replicationFactor = namespaceWarmUp.getReplicationFactor();
    this.fileSize = namespaceWarmUp.getFileSize();
    this.baseDir = namespaceWarmUp.getBaseDir();
    List workers = new ArrayList<WarmUp>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new WarmUp(namespaceWarmUp.getFilesToCreate(), replicationFactor,
              fileSize, baseDir);
      workers.add(worker);
    }
    executor.invokeAll(workers); // blocking call
    return new NamespaceWarmUp.Response();
  }

  public class WarmUp implements Callable {

    private DistributedFileSystem dfs;
    private FilePool filePool;
    private int filesToCreate;
    private short replicationFactor;
    private long fileSize;
    private String baseDir;

    public WarmUp(int filesToCreate, short replicationFactor, long fileSize, String baseDir) throws IOException {
      this.filesToCreate = filesToCreate;
      this.fileSize = fileSize;
      this.replicationFactor = replicationFactor;
      this.baseDir = baseDir;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, baseDir);
      String filePath = null;

      for (int i = 0; i < filesToCreate; i++) {
        try {
          filePath = filePool.getFileToCreate();
          BenchmarkUtils
                  .createFile(dfs, new Path(filePath), replicationFactor,
                  fileSize);
          filePool.fileCreationSucceeded(filePath);
          BenchmarkUtils.readFile(dfs, new Path(filePath), fileSize);
        } catch (Exception e) {
          Logger.error(e);

        }
      }
      return null;
    }
  };

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
    executor.invokeAll(workers);// blocking call
    long phaseFinishTime = System.currentTimeMillis();
    long actualExecutionTime = (phaseFinishTime - phaseStartTime);
    
    double speed = ((double) successfulOps.get() / (double) actualExecutionTime); // p / ms
    speed = speed * 1000;

    RawBenchmarkCommand.Response response =
            new RawBenchmarkCommand.Response(opType,
            actualExecutionTime, successfulOps.get(), failedOps.get(), speed);
    return response;
  }

  public class Generic implements Callable {

    private BenchmarkOperations opType;
    private DistributedFileSystem dfs;
    private FilePool filePool;
    private String baseDir;

    public Generic(String baseDir, BenchmarkOperations opType) throws IOException {
      this.baseDir = baseDir;
      this.opType = opType;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, baseDir);

      while (true) {
        try {

          String path = getPath();

          if (path == null || ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS))) {
            return null;
          }
          
          performOp(path);

          successfulOps.incrementAndGet();

          if (Logger.canILog()) {
            Logger.printMsg("Successful " + opType + " ops " + successfulOps.get() + " Failed ops " + failedOps.get() + " Speed: " + speedPSec(successfulOps, phaseStartTime));
          }
        } catch (Exception e) {
          failedOps.incrementAndGet();
          Logger.error(e);
        }
      }
    }

    private String getPath() {
      String path = null;
      if (opType == BenchmarkOperations.SET_REPLICATION) {
        path = filePool.getFileToSetReplication();
      } else if (opType == BenchmarkOperations.FILE_INFO) {
        path = filePool.getFileToInfo();
      } else if (opType == BenchmarkOperations.DIR_INFO) {
        path = filePool.getDirToInfo();
      } else if (opType == BenchmarkOperations.CHMOD_DIR) {
        path = filePool.getDirPathToChangePermissions();
      } else if (opType == BenchmarkOperations.CHMOD_FILE) {
        path = filePool.getFilePathToChangePermissions();
      } else if (opType == BenchmarkOperations.LS_FILE) {
        path = filePool.getFileToStat();
      } else if (opType == BenchmarkOperations.LS_DIR) {
        path = filePool.getDirToStat();
      } else if (opType == BenchmarkOperations.READ_FILE) {
        path = filePool.getFileToRead();
      } else if (opType == BenchmarkOperations.MKDIRS) {
        path = filePool.getDirToCreate();
      } else if (opType == BenchmarkOperations.CREATE_FILE) {
        path = filePool.getFileToCreate();
      } else if (opType == BenchmarkOperations.DELETE_FILE) {
        path = filePool.getFileToDelete();
      } else if (opType == BenchmarkOperations.RENAME_FILE) {
        path = filePool.getFileToRename();
      } else{
        throw new IllegalStateException("Fucked");
      }
      
      System.out.println(opType+" Path: "+path);
      return path;
    }

    private void performOp(String pathStr) throws IOException {
      Path path = new Path(pathStr);
      if (opType == BenchmarkOperations.SET_REPLICATION) {
        BenchmarkUtils.setReplication(dfs, path);
      } else if (opType == BenchmarkOperations.FILE_INFO
              || opType == BenchmarkOperations.DIR_INFO) {
        BenchmarkUtils.getInfo(dfs, path);
      } else if (opType == BenchmarkOperations.CHMOD_FILE
              || opType == BenchmarkOperations.CHMOD_DIR) {
        BenchmarkUtils.chmodPath(dfs, path);
      } else if (opType == BenchmarkOperations.LS_FILE
              || opType == BenchmarkOperations.LS_DIR) {
        BenchmarkUtils.ls(dfs, path);
      } else if (opType == BenchmarkOperations.READ_FILE) {
        BenchmarkUtils.readFile(dfs, path, fileSize);
      } else if (opType == BenchmarkOperations.MKDIRS) {
        BenchmarkUtils.mkdirs(dfs, path);
      } else if (opType == BenchmarkOperations.CREATE_FILE) {
          BenchmarkUtils.createFile(dfs, path, replicationFactor, fileSize);
          filePool.fileCreationSucceeded(pathStr);
      } else if (opType == BenchmarkOperations.DELETE_FILE) {
        BenchmarkUtils.deleteFile(dfs, path);
      } else if (opType == BenchmarkOperations.RENAME_FILE) {
        String from = filePool.getFileToRename();
        String to = from + "_rnd";
        if (BenchmarkUtils.renameFile(dfs, new Path(from), new Path(to))) {
          filePool.fileRenamed(from, to);
        }
      } else {
        throw new IllegalStateException("Fucked");
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
