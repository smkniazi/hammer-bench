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
package io.hops.experiments.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.experiments.benchmarks.blockreporting.BlockReportingBenchmarkCommand;
import io.hops.experiments.benchmarks.blockreporting.BlockReportingWarmUp;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.controller.commands.Handshake;
import io.hops.experiments.controller.commands.KillSlave;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmarkCommand;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmarkCreateCommand;
import io.hops.experiments.benchmarks.common.commands.NamespaceWarmUp;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.blockreporting.BlockReportBMResults;
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import io.hops.experiments.benchmarks.rawthroughput.RawBMResults;
import io.hops.experiments.controller.config.ConfigKeys;
import io.hops.experiments.controller.config.Configuration;
import io.hops.experiments.results.compiler.InterleavedBMResultsAggregator;
import io.hops.experiments.results.compiler.RawBMResultAggregator;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class Master {

  Set<InetAddress> misbehavingSlaves = new HashSet<InetAddress>();
  Map<InetAddress, SlaveConnection> slavesConnections = new HashMap<InetAddress, SlaveConnection>();
  List<BMResult> results = new ArrayList<BMResult>();
  Configuration config;

  public static void main(String[] argv) throws Exception {
    String configFilePath = "master.properties";
    if (argv.length == 1) {
      if (argv[0].compareToIgnoreCase("help") == 0) {
        Configuration.printHelp();
        System.exit(0);
      } else {
        configFilePath = argv[0];
      }
    }
    new Master().start(configFilePath);
  }

  public void start(String configFilePath) throws Exception {
    try {
      System.out.println("*** Starting the master ***");
      config = new Configuration(configFilePath);

      removeExistingResultsFiles();
      
      startRemoteLogger(config.getSlavesList().size());

      connectSlaves();

      handShakeWithSlaves(); // Let all the clients know show is the master

      warmUpSlaves();

      //start the commander
      startCommander();

      generateResultsFile();
      
      printAllResults();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      sendToAllSlaves(new KillSlave(), 0/*delay*/);
      System.exit(0);
    }
  }

  private void startRemoteLogger(int maxSlaves) {
    Logger.LogListener listener = new Logger.LogListener(config.getRemoteLogginPort(),maxSlaves);
    Thread thread = new Thread(listener);
    thread.start();
    System.out.println("Logger started.");
  }

  private void startCommander() throws IOException, InterruptedException, ClassNotFoundException {
    if (config.getBenchMarkType() == BenchmarkType.RAW) {
      startRawCommander();
    } else if (config.getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      startInterleavedCommander();
    } else if (config.getBenchMarkType() == BenchmarkType.BR) {
      startBlockReportingCommander();
    } else {
      throw new IllegalStateException("Unsupported Benchmark ");
    }

  }

  private void startRawCommander() throws IOException, InterruptedException, ClassNotFoundException {

    if (config.getRawBmMkdirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.MKDIRS, config.getRawBmMkdirPhaseDuration()));
    }

    if (config.getRawBmFilesCreationPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCreateCommand.Request(
              config.getRawBmMaxFilesToCreate(),
              BenchmarkOperations.CREATE_FILE,
              config.getRawBmFilesCreationPhaseDuration()));
    }

    if (config.getRawBmAppendFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.APPEND_FILE,
              config.getRawBmAppendFilePhaseDuration()));
    }

    if (config.getRawBmReadFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.READ_FILE,
              config.getRawBmReadFilesPhaseDuration()));
    }

    if (config.getRawBmLsFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_FILE,
              config.getRawBmLsFilePhaseDuration()));
    }

    if (config.getRawBmLsDirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_DIR,
              config.getRawBmLsDirPhaseDuration()));
    }

    if (config.getRawBmChmodFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_FILE,
              config.getRawBmChmodFilesPhaseDuration()));
    }

    if (config.getRawBmChmodDirsPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_DIR,
              config.getRawBmChmodDirsPhaseDuration()));
    }

    if (config.getRawBmSetReplicationPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.SET_REPLICATION,
              config.getRawBmSetReplicationPhaseDuration()));
    }

    if (config.getRawBmGetFileInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.FILE_INFO,
              config.getRawBmGetFileInfoPhaseDuration()));
    }

    if (config.getRawBmGetDirInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DIR_INFO,
              config.getRawBmGetDirInfoPhaseDuration()));
    }


    if (config.getRawFileChangeUserPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHOWN_FILE,
              config.getRawFileChangeUserPhaseDuration()));
    }


    if (config.getRawDirChangeUserPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHOWN_DIR,
              config.getRawDirChangeUserPhaseDuration()));
    }


    if (config.getRawBmRenameFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.RENAME_FILE,
              config.getRawBmRenameFilesPhaseDuration()));
    }

    if (config.getRawBmDeleteFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DELETE_FILE,
              config.getRawBmDeleteFilesPhaseDuration()));
    }
  }

  private void startBlockReportingCommander() throws IOException, ClassNotFoundException {
    System.out.println("Starting BlockReporting Benchmark ...");
    prompt();
    BlockReportingBenchmarkCommand.Request request = new BlockReportingBenchmarkCommand.Request(config
            .getBlockReportBenchMarkDuration(), config
            .getBlockReportingMinTimeBeforeNextReport(), config
            .getBlockReportingMaxTimeBeforeNextReport());

    sendToAllSlaves(request, 0/*delay*/);

    Collection<Object> responses = receiveFromAllSlaves(Integer.MAX_VALUE);
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics avgTimePerReport = new DescriptiveStatistics();
    DescriptiveStatistics avgTimeTogetANewNameNode = new DescriptiveStatistics();
    DescriptiveStatistics noOfNNs = new DescriptiveStatistics();

    for (Object obj : responses) {
      if (!(obj instanceof BlockReportingBenchmarkCommand.Response)) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        BlockReportingBenchmarkCommand.Response response = (BlockReportingBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getSuccessfulOps());
        failedOps.addValue(response.getFailedOps());
        speed.addValue(response.getSpeed());
        avgTimePerReport.addValue(response.getAvgTimePerReport());
        avgTimeTogetANewNameNode.addValue(response.getAvgTimeTogetNewNameNode());
        noOfNNs.addValue(response.getNnCount());
      }
    }

    BlockReportBMResults result = new BlockReportBMResults(config.getNamenodeCount(),
            (int)Math.floor(noOfNNs.getMean()),
            config.getNdbNodesCount(),
            speed.getSum(), successfulOps.getSum(),
            failedOps.getSum(), avgTimePerReport.getMean(), avgTimeTogetANewNameNode.getMean());

    printMasterResultMessages(result);
  }

  private void startInterleavedCommander() throws IOException, ClassNotFoundException, InterruptedException {
    System.out.println("Starting Interleaved Benchmark ...");
    prompt();
    InterleavedBenchmarkCommand.Request request =
            new InterleavedBenchmarkCommand.Request(config);
    sendToAllSlaves(request, 0/*delay*/);

    Thread.sleep(config.getInterleavedBmDuration());
    Collection<Object> responses = receiveFromAllSlaves(60 * 1000 /*sec wait*/);
    InterleavedBMResults result = InterleavedBMResultsAggregator.processInterleavedResults(responses, config);
    printMasterResultMessages(result);
  }

  private void handShakeWithSlaves() throws IOException, ClassNotFoundException {
    //send request
    printMasterLogMessages("Starting Hand Shake Protocol");
    prompt();
    sendHandshakeToAllSlaves(
            new Handshake.Request(config.getSlaveNumThreads(), config.getFileSize(), config.getAppendFileSize(),
            config.getReplicationFactor(), config.getBenchMarkType(),
            config.getBaseDir(),
            config.isEnableRemoteLogging(), config.getRemoteLogginPort(),
            config.getDirPerDir(),
            config.getFilesPerDir(), config.getRawBmMaxFilesToCreate(),
            config.isFixedDepthTree(), config.getTreeDepth(),
            config.getBenchMarkFileSystemName(), config.getFsConfig()));
    Collection<Object> allResponses = receiveFromAllSlaves(60 * 1000 /*sec wait*/);

    for (Object response : allResponses) {
      if (!(response instanceof Handshake.Response)) {
        throw new IllegalStateException("Disobedient slave. Sent me something other than hand shake response");
      }
    }
    printMasterLogMessages("Hand Shanke With All Slave Completed");
  }

  private void warmUpSlaves()
          throws IOException, ClassNotFoundException, SQLException {
    printMasterLogMessages("Warming Up ... ");
    prompt();
    WarmUpCommand.Request warmUpCommand = null;
    if (config.getBenchMarkType() == BenchmarkType.INTERLEAVED
            || config.getBenchMarkType() == BenchmarkType.RAW) {
      warmUpCommand = new NamespaceWarmUp.Request(config.getBenchMarkType(), config.getFilesToCreateInWarmUpPhase(), config.getReplicationFactor(),
              config.getFileSize(), config.getAppendFileSize(),
              config.getBaseDir());
    } else if (config.getBenchMarkType() == BenchmarkType.BR) {
      warmUpCommand = new BlockReportingWarmUp.Request(config.getBaseDir(), config.getBlockReportingNumOfBlocksPerReport(), config
              .getBlockReportingNumOfBlocksPerFile(), config
              .getBlockReportingNumOfFilesPerDir(), config
              .getReplicationFactor(), config.getBlockReportingMaxBlockSize(), config.isBlockReportingSkipCreations(), config.getBlockReportingPersistDatabase());
    } else {
      throw new UnsupportedOperationException("Wrong Benchmark type for"
              + " warm up " + config.getBenchMarkType());
    }

    sendToAllSlaves(warmUpCommand, config.getSlaveWarmUpDelay()/*delay*/);

    Collection<Object> allResponses = receiveFromAllSlaves(config.getWarmUpPhaseWaitTime());

    for (Object response : allResponses) {
      if (!(response instanceof WarmUpCommand.Response)) {
        throw new IllegalStateException("Disobedient slave. Sent me something other than hand shake response");
      }
    }
    printMasterLogMessages("All Slaves Warmed Up");
  }

  public void startRawBenchmarkPhase(RawBenchmarkCommand.Request request) throws IOException, InterruptedException, ClassNotFoundException {
    printMasterLogMessages("Starting " + request.getPhase() + " using "
            + config.getSlaveNumThreads() * config.getSlavesList().size()
            + " client(s). Time phase duration "
            + request.getDurationInMS() / (double) (1000 * 60) + " mins");
    prompt();

    sendToAllSlaves(request,0/*delay*/);

    Thread.sleep(request.getDurationInMS());
    Collection<Object> responses = receiveFromAllSlaves(60 * 1000/*sec wait*/);

    RawBMResults result = RawBMResultAggregator.processSlaveResponses(responses, request, config);
    printMasterResultMessages(result);
  }

  private void connectSlaves() throws IOException {
    if (config != null) {
      List<InetAddress> slaves = config.getSlavesList();
      for (InetAddress slave : slaves) {
        printMasterLogMessages("Connecting to slave " + slave);
        try {
          SlaveConnection slaveConn = new SlaveConnection(slave, config.getSlaveListeningPort());
          slavesConnections.put(slave, slaveConn);
        } catch (Exception e) {
          misbehavingSlaves.add(slave);
          printMasterLogMessages("*** ERROR  unable to connect " + slave);
        }
      }
      if (misbehavingSlaves.size() > config.getMaxSlavesFailureThreshold()) {
        printMasterLogMessages("*** Too many slaves failed. Abort test. Failed Slaves Count "+misbehavingSlaves.size()+" Threshold: "+ config.getMaxSlavesFailureThreshold());
        System.exit(-1);
      }
    }
  }

  private void sendHandshakeToAllSlaves(Handshake.Request handshake) throws
          IOException {
    if (!slavesConnections.isEmpty()) {
      int slaveId = 0;
      for (InetAddress slave : slavesConnections.keySet()) {
        SlaveConnection conn = slavesConnections.get(slave);
        handshake.setSlaveId(slaveId++);
        conn.sendToSlave(handshake);
      }
    }
  }

  private void sendToAllSlaves(Object obj, int delay) throws IOException {
    if (!slavesConnections.isEmpty()) {
      for (InetAddress slave : slavesConnections.keySet()) {
        SlaveConnection conn = slavesConnections.get(slave);
        conn.sendToSlave(obj);
        try{
          Thread.sleep(delay);
        }catch(InterruptedException e){}
      }
    }
  }

  private Collection<Object> receiveFromAllSlaves(int timeout) throws ClassNotFoundException, UnknownHostException, IOException {
    Map<InetAddress, Object> responses = new HashMap<InetAddress, Object>();
    if (!slavesConnections.isEmpty()) {
      for (InetAddress slave : slavesConnections.keySet()) {
        SlaveConnection conn = slavesConnections.get(slave);
        Object obj = conn.recvFromSlave(timeout);
        if (obj != null) {
          responses.put(slave, obj);
        }
      }
    }
    return responses.values();
  }

  private void prompt() throws IOException {
    if (!config.isSkipAllPrompt()) {
      printMasterLogMessages("Press Enter to start ");
      System.in.read();
    }
  }

  private void printMasterLogMessages(String msg) {
    redColoredText(msg);
  }

  private void printMasterResultMessages(BMResult result) throws FileNotFoundException, IOException {
    blueColoredText(result.toString());
    results.add(result);
  }
  
  private void removeExistingResultsFiles() throws IOException{
    File dir = new File(config.getResultsDir());
    if(dir.exists()){
       FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }

  private void generateResultsFile() throws FileNotFoundException, IOException {
    
    String filePath = config.getResultsDir();
    if(!filePath.endsWith("/")){
      filePath += "/";
    }
    filePath += ConfigKeys.BINARY_RESULT_FILE_NAME;
    printMasterLogMessages("Writing results to "+filePath);
    FileOutputStream fout = new FileOutputStream(filePath);
    ObjectOutputStream oos = new ObjectOutputStream(fout);
    for (BMResult result : results) {
      oos.writeObject(result);
    }
    oos.close();
    
    
    filePath = config.getResultsDir();
    if(!filePath.endsWith("/")){
      filePath += "/";
    }
    filePath += ConfigKeys.TEXT_RESULT_FILE_NAME;
    printMasterLogMessages("Writing results to "+filePath);
    FileWriter out = new FileWriter(filePath, false);
    for (BMResult result : results) {
      out.write(result.toString() + "\n");
    }
    out.close();
  }

  private void redColoredText(String msg) {
    System.out.println((char) 27 + "[31m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  public static void blueColoredText(String msg) {
    System.out.println((char) 27 + "[36m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  private void printAllResults() throws FileNotFoundException, IOException {
    System.out.println("\n\n\n");
    System.out.println("************************ All Results ************************");
    System.out.println("\n\n\n");
    
    String filePath = config.getResultsDir();
    if(!filePath.endsWith("/")){
      filePath += "/";
    }
    filePath += ConfigKeys.TEXT_RESULT_FILE_NAME;
    
    printMasterLogMessages("Reading results from "+filePath);
    BufferedReader br = new BufferedReader(new FileReader(filePath));
    try {

      String line = br.readLine();

      while (line != null) {
        blueColoredText(line);
        line = br.readLine();
      }
    } finally {
      br.close();
    }
    System.out.println("\n\n\n");
  }

  public class SlaveConnection {

    private final Socket socket;

    SlaveConnection(InetAddress slaveInetAddress, int slavePort) throws IOException {
      socket = new Socket(slaveInetAddress, slavePort);
    }

    public void sendToSlave(Object obj) {

      if (isSlaveHealthy(socket.getInetAddress())) {
        try {
          printMasterLogMessages("SEND " + obj.getClass().getCanonicalName() + " to " + socket.getInetAddress());
          socket.setSendBufferSize(ConfigKeys.BUFFER_SIZE);
          ObjectOutputStream sendToSlave = new ObjectOutputStream(socket.getOutputStream());
          sendToSlave.writeObject(obj);
        } catch (Exception e) {
          handleMisBehavingSlave(socket.getInetAddress());
        }
      } else {
        printMasterLogMessages("*** ERROR send request to " + socket.getInetAddress() + " is ignored ");
      }
    }

    public Object recvFromSlave(int timeout) {
      if (isSlaveHealthy(socket.getInetAddress())) {
        try {
          socket.setSoTimeout(timeout);
          socket.setReceiveBufferSize(ConfigKeys.BUFFER_SIZE);
          ObjectInputStream recvFromSlave = new ObjectInputStream(socket.getInputStream());
          Object obj = recvFromSlave.readObject();
          printMasterLogMessages("RECVD " + obj.getClass().getCanonicalName() + " from " + socket.getInetAddress());
          socket.setSoTimeout(Integer.MAX_VALUE);
          return obj;
        } catch (Exception e) {
          handleMisBehavingSlave(socket.getInetAddress());
          return null;
        }
      } else {
        printMasterLogMessages("*** ERROR recv request from " + socket.getInetAddress() + " is ignored ");
        return null;
      }
    }

    private void handleMisBehavingSlave(InetAddress slave) {
      misbehavingSlaves.add(slave);
      printMasterLogMessages("*** Slaved Failed. " + slave);
      if (misbehavingSlaves.size() > config.getMaxSlavesFailureThreshold()) {
        printMasterLogMessages("*** HARD ERROR. Too many slaves failed. ABORT Test.");
        System.exit(-1);
      }
    }

    private boolean isSlaveHealthy(InetAddress slave) {
      if (!misbehavingSlaves.contains(slave)) {
        return true;
      } else {
        return false;
      }
    }
  }
}
