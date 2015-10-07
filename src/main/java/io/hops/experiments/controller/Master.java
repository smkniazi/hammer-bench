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

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Doubles;
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
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 *
 * @author salman
 */
public class Master {

  Set<InetAddress> misbehavingSlaves = new HashSet<InetAddress>();
  Map<InetAddress, SlaveConnection> slavesConnections = new HashMap<InetAddress, SlaveConnection>();
  List<BMResult> results = new ArrayList<BMResult>();

  public static void main(String[] argv) throws Exception {
    String configFilePath = "master.properties";
    if (argv.length == 1) {
      if (argv[0].compareToIgnoreCase("help") == 0) {
        MasterArgsReader.printHelp();
        System.exit(0);
      } else {
        configFilePath = argv[0];
      }
    }
    new Master().start(configFilePath);
  }
  MasterArgsReader args;

  public void start(String configFilePath) throws Exception {
    try {
      System.out.println("*** Starting the master ***");
      args = new MasterArgsReader(configFilePath);

      removeExistingResultsFiles();
      
      startRemoteLogger();

      connectSlaves();

      handShakeWithSlaves(); // Let all the clients know show is the master

      //send init to slaves
      warmUpSlaves();

      //start the commander
      startCommander();

      generateResultsFile();
      
      printAllResults();
    } catch (Exception e) {
      System.err.println(e);
      e.printStackTrace();
    } finally {
      sendToAllSlaves(new KillSlave());
      System.exit(0);
    }
  }

  private void startRemoteLogger() {
    Logger.LogListener listener = new Logger.LogListener(args.getRemoteLogginPort());
    Thread thread = new Thread(listener);
    thread.start();
    System.out.println("Logger started.");
  }

  private void startCommander() throws IOException, InterruptedException, ClassNotFoundException {
    if (args.getBenchMarkType() == BenchmarkType.RAW) {
      startRawCommander();
    } else if (args.getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      startInterleavedCommander();
    } else if (args.getBenchMarkType() == BenchmarkType.BR) {
      startBlockReportingCommander();
    } else {
      throw new IllegalStateException("Unsupported Benchmark ");
    }

  }

  private void startRawCommander() throws IOException, InterruptedException, ClassNotFoundException {

    if (args.getRawBmMkdirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.MKDIRS, args.getRawBmMkdirPhaseDuration()));
    }

    if (args.getRawBmFilesCreationPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCreateCommand.Request(
              args.getRawBmMaxFilesToCreate(),
              BenchmarkOperations.CREATE_FILE,
              args.getRawBmFilesCreationPhaseDuration()));
    }

    if (args.getRawBmAppendFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.APPEND_FILE,
              args.getRawBmAppendFilePhaseDuration()));
    }

    if (args.getRawBmReadFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.READ_FILE,
              args.getRawBmReadFilesPhaseDuration()));
    }

    if (args.getRawBmLsFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_FILE,
              args.getRawBmLsFilePhaseDuration()));
    }

    if (args.getRawBmLsDirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_DIR,
              args.getRawBmLsDirPhaseDuration()));
    }

    if (args.getRawBmChmodFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_FILE,
              args.getRawBmChmodFilesPhaseDuration()));
    }

    if (args.getRawBmChmodDirsPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_DIR,
              args.getRawBmChmodDirsPhaseDuration()));
    }

    if (args.getRawBmSetReplicationPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.SET_REPLICATION,
              args.getRawBmSetReplicationPhaseDuration()));
    }

    if (args.getRawBmGetFileInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.FILE_INFO,
              args.getRawBmGetFileInfoPhaseDuration()));
    }

    if (args.getRawBmGetDirInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DIR_INFO,
              args.getRawBmGetDirInfoPhaseDuration()));
    }


    if (args.getRawFileChangeUserPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHOWN_FILE,
              args.getRawFileChangeUserPhaseDuration()));
    }


    if (args.getRawDirChangeUserPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHOWN_DIR,
              args.getRawDirChangeUserPhaseDuration()));
    }


    if (args.getRawBmRenameFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.RENAME_FILE,
              args.getRawBmRenameFilesPhaseDuration()));
    }

    if (args.getRawBmDeleteFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DELETE_FILE,
              args.getRawBmDeleteFilesPhaseDuration()));
    }
  }

  private void startBlockReportingCommander() throws IOException, ClassNotFoundException {
    System.out.println("Starting BlockReporting Benchmark ...");
    prompt();
    BlockReportingBenchmarkCommand.Request request = new BlockReportingBenchmarkCommand.Request(args
            .getBLockReportingNumOfReports(), args
            .getBlockReportingMinTimeBeforeNextReport(), args
            .getBlockReportingMaxTimeBeforeNextReport());

    sendToAllSlaves(request);

    Collection<Object> responses = receiveFromAllSlaves(Integer.MAX_VALUE);
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics avgTimePerReport = new DescriptiveStatistics();
    DescriptiveStatistics avgTimeTogetANewNameNode = new DescriptiveStatistics();

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
      }
    }

    BlockReportBMResults result = new BlockReportBMResults(args.getNamenodeCount(),
            args.getNdbNodesCount(),
            speed.getSum(), successfulOps.getSum(),
            failedOps.getSum(), avgTimePerReport.getMean(), avgTimeTogetANewNameNode.getMean());

    printMasterResultMessages(result);
  }

  private void startInterleavedCommander() throws IOException, ClassNotFoundException, InterruptedException {
    System.out.println("Starting Interleaved Benchmark ...");
    prompt();
    InterleavedBenchmarkCommand.Request request =
            new InterleavedBenchmarkCommand.Request(args.getInterleavedBmCreateFilesPercentage(),
            args.getInterleavedBmAppendFilePercentage(),
            args.getInterleavedBmReadFilesPercentage(), args.getInterleavedBmRenameFilesPercentage(), args.getInterleavedBmDeleteFilesPercentage(),
            args.getInterleavedBmLsFilePercentage(), args.getInterleavedBmLsDirPercentage(),
            args.getInterleavedBmChmodFilesPercentage(), args.getInterleavedBmChmodDirsPercentage(),
            args.getInterleavedBmMkdirPercentage(),
            args.getInterleavedBmSetReplicationPercentage(),
            args.getInterleavedBmGetFileInfoPercentage(),
            args.getInterleavedBmGetDirInfoPercentage(),
            args.getInterleavedFileChangeUserPercentage(),
            args.getInterleavedDirChangeUserPercentage(),
            args.getInterleavedBmDuration(), args.getFileSize(), args.getAppendFileSize(),
            args.getReplicationFactor(), args.getBaseDir(), args.isPercentileEnabled());
    sendToAllSlaves(request);

    Thread.sleep(args.getInterleavedBmDuration());
    Collection<Object> responses = receiveFromAllSlaves(20 * 1000 /*sec wait*/);
    InterleavedBMResults result = InterleavedBMResultsAggregator.processInterleavedResults(responses,args);
    printMasterResultMessages(result);
  }

  private void handShakeWithSlaves() throws IOException, ClassNotFoundException {
    //send request
    printMasterLogMessages("Starting Hand Shake Protocol");
    prompt();
    sendHandshakeToAllSlaves(
            new Handshake.Request(args.getSlaveNumThreads(), args.getFileSize(), args.getAppendFileSize(),
            args.getReplicationFactor(), args.getBenchMarkType(),
            args.getBaseDir(),
            args.isEnableRemoteLogging(), args.getRemoteLogginPort(),
            args.getNameNodeRpcAddress(), args.getNameNodeSelectorPolicy(),
            args.getNameNodeRefreshRate(), args.getDirPerDir(), 
            args.getFilesPerDir(),args.getRawBmMaxFilesToCreate(),
            args.isFixedDepthTree(), args.getTreeDepth(), 
            args.getBenchMarkFileSystemName()));
    Collection<Object> allResponses = receiveFromAllSlaves(10 * 1000 /*sec wait*/);

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
    if (args.getBenchMarkType() == BenchmarkType.INTERLEAVED
            || args.getBenchMarkType() == BenchmarkType.RAW) {
      warmUpCommand = new NamespaceWarmUp.Request(args.getBenchMarkType(), args.getFilesToCreateInWarmUpPhase(), args.getReplicationFactor(),
              args.getFileSize(), args.getAppendFileSize(),
              args.getBaseDir());
    } else if (args.getBenchMarkType() == BenchmarkType.BR) {
      warmUpCommand = new BlockReportingWarmUp.Request(args.getBaseDir(), args.getBlockReportingNumOfBlocksPerReport(), args
              .getBlockReportingNumOfBlocksPerFile(), args
              .getBlockReportingNumOfFilesPerDir(), args
              .getReplicationFactor(), args.getBlockReportingMaxBlockSize(), args.isBlockReportingSkipCreations(), args.getBlockReportingPersistDatabase());
    } else {
      throw new UnsupportedOperationException("Wrong Benchmark type for"
              + " warm up " + args.getBenchMarkType());
    }

    sendToAllSlaves(warmUpCommand);

    Collection<Object> allResponses = receiveFromAllSlaves(args.getWarmUpPhaseWaitTime());

    for (Object response : allResponses) {
      if (!(response instanceof WarmUpCommand.Response)) {
        throw new IllegalStateException("Disobedient slave. Sent me something other than hand shake response");
      }
    }
    printMasterLogMessages("All Slaves Warmed Up");
  }

  public void startRawBenchmarkPhase(RawBenchmarkCommand.Request request) throws IOException, InterruptedException, ClassNotFoundException {
    printMasterLogMessages("Starting " + request.getPhase() + " using "
            + args.getSlaveNumThreads() * args.getSlavesList().size()
            + " client(s). Time phase duration "
            + request.getDurationInMS() / (double) (1000 * 60) + " mins");
    prompt();

    sendToAllSlaves(request);

    Thread.sleep(request.getDurationInMS());
    Collection<Object> responses = receiveFromAllSlaves(10 * 1000/*sec wait*/);

    RawBMResults result = RawBMResultAggregator.processSlaveResponses(responses, request, args);
    printMasterResultMessages(result);
  }

  private void connectSlaves() throws IOException {
    if (args != null) {
      List<InetAddress> slaves = args.getSlavesList();
      for (InetAddress slave : slaves) {
        printMasterLogMessages("Connecting to slave " + slave);
        try {
          SlaveConnection slaveConn = new SlaveConnection(slave, args.getSlaveListeningPort());
          slavesConnections.put(slave, slaveConn);
        } catch (Exception e) {
          misbehavingSlaves.add(slave);
          printMasterLogMessages("*** ERROR  unable to connect " + slave);
        }
      }
      if (misbehavingSlaves.size() > args.getMaxSlavesFailureThreshold()) {
        printMasterLogMessages("*** Too many slaves failed. Abort test. Failed Slaves Count "+misbehavingSlaves.size()+" Threshold: "+args.getMaxSlavesFailureThreshold());
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

  private void sendToAllSlaves(Object obj) throws IOException {
    if (!slavesConnections.isEmpty()) {
      for (InetAddress slave : slavesConnections.keySet()) {
        SlaveConnection conn = slavesConnections.get(slave);
        conn.sendToSlave(obj);
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
    if (!args.isSkipAllPrompt()) {
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
    File dir = new File(args.getResultsDir());
    if(dir.exists()){
       FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }

  private void generateResultsFile() throws FileNotFoundException, IOException {
    
    String filePath = args.getResultsDir();
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
    
    
    filePath = args.getResultsDir();
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

  private void blueColoredText(String msg) {
    System.out.println((char) 27 + "[36m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  private void printAllResults() throws FileNotFoundException, IOException {
    System.out.println("\n\n\n");
    System.out.println("************************ All Results ************************");
    System.out.println("\n\n\n");
    
    String filePath = args.getResultsDir();
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
      if (misbehavingSlaves.size() > args.getMaxSlavesFailureThreshold()) {
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
