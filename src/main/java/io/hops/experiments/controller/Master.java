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
import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.results.BMResults;
import io.hops.experiments.results.BlockReportBMResults;
import io.hops.experiments.results.InterleavedBMResults;
import io.hops.experiments.results.RawBMResults;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class Master {

  Set<InetAddress> misbehavingSlaves = new HashSet<InetAddress>();
  Map<InetAddress, SlaveConnection> slavesConnections = new HashMap<InetAddress, SlaveConnection>();
  List<BMResults> results = new ArrayList<BMResults>();

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

      resetResultFile();

      startRemoteLogger();

      connectSlaves();

      handShakeWithSlaves(); // Let all the clients know show is the master

      //send init to slaves
      warmUpSlaves();

      //start the commander
      startCommander();

      printAllResults();

      generateBinaryFile();

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
    }
  }

  private void startRawCommander() throws IOException, InterruptedException, ClassNotFoundException {

    if (args.getRawMkdirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.MKDIRS, args.getRawMkdirPhaseDuration()));
    }

    if (args.getRawCreateFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCreateCommand.Request(
              args.getRawCreatePhaseMaxFilesToCreate(),
              BenchmarkOperations.CREATE_FILE,
              args.getRawCreateFilesPhaseDuration()));
    }

    if (args.getAppendFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.APPEND_FILE,
              args.getAppendFilePhaseDuration()));
    }

    if (args.getRawReadFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.READ_FILE,
              args.getRawReadFilesPhaseDuration()));
    }

    if (args.getRawLsFilePhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_FILE,
              args.getRawLsFilePhaseDuration()));
    }

    if (args.getRawLsDirPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.LS_DIR,
              args.getRawLsDirPhaseDuration()));
    }

    if (args.getRawChmodFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_FILE,
              args.getRawChmodFilesPhaseDuration()));
    }

    if (args.getRawChmodDirsPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.CHMOD_DIR,
              args.getRawChmodDirsPhaseDuration()));
    }

    if (args.getRawSetReplicationPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.SET_REPLICATION,
              args.getRawSetReplicationPhaseDuration()));
    }

    if (args.getRawGetFileInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.FILE_INFO,
              args.getRawGetFileInfoPhaseDuration()));
    }

    if (args.getRawGetDirInfoPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DIR_INFO,
              args.getRawGetDirInfoPhaseDuration()));
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


    if (args.getRawRenameFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.RENAME_FILE,
              args.getRawRenameFilesPhaseDuration()));
    }

    if (args.getRawDeleteFilesPhaseDuration() > 0) {
      startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
              BenchmarkOperations.DELETE_FILE,
              args.getRawDeleteFilesPhaseDuration()));
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
            args.getNoOfNDBDataNodes(),
            speed.getSum(), successfulOps.getSum(),
            failedOps.getSum(), avgTimePerReport.getMean(), avgTimeTogetANewNameNode.getMean());

    printMasterResultMessages(result);
  }

  private void startInterleavedCommander() throws IOException, ClassNotFoundException, InterruptedException {
    System.out.println("Starting Interleaved Benchmark ...");
    prompt();
    InterleavedBenchmarkCommand.Request request =
            new InterleavedBenchmarkCommand.Request(args.getInterleavedCreateFilesPercentage(),
            args.getInterleavedAppendFilePercentage(),
            args.getInterleavedReadFilesPercentage(), args.getInterleavedRenameFilesPercentage(), args.getInterleavedDeleteFilesPercentage(),
            args.getInterleavedLsFilePercentage(), args.getInterleavedLsDirPercentage(),
            args.getInterleavedChmodFilesPercentage(), args.getInterleavedChmodDirsPercentage(),
            args.getInterleavedMkdirPercentage(),
            args.getInterleavedSetReplicationPercentage(),
            args.getInterleavedGetFileInfoPercentage(),
            args.getInterleavedGetDirInfoPercentage(),
            args.getInterleavedFileChangeUserPercentage(),
            args.getInterleavedDirChangeUserPercentage(),
            args.getInterleavedBMDuration(), args.getFileSize(), args.getAppendFileSize(),
            args.getReplicationFactor(), args.getBaseDir());
    sendToAllSlaves(request);

    Thread.sleep(args.getInterleavedBMDuration());
    Collection<Object> responses = receiveFromAllSlaves(20 * 1000 /*sec wait*/);
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics duration = new DescriptiveStatistics();
    for (Object obj : responses) {
      if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getTotalSuccessfulOps());
        failedOps.addValue(response.getTotalFailedOps());
        speed.addValue(response.getOpsPerSec());
        duration.addValue(response.getRunTime());
      }
    }

    InterleavedBMResults result = new InterleavedBMResults(args.getNamenodeCount(),
            args.getNoOfNDBDataNodes(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))), (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()));
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
            args.getNameNodeRefreshRate(), args.getDirPerDir(), args.getFilesPerDir()));
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
    if (args.getBenchMarkType() == BenchmarkType.INTERLEAVED || args
            .getBenchMarkType() == BenchmarkType.RAW) {
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
            + args.getSlaveNumThreads() * args.getListOfSlaves().size()
            + " client(s). Time phase duration "
            + request.getDurationInMS() / (double) (1000 * 60) + " mins");
    prompt();

    sendToAllSlaves(request);

    Thread.sleep(request.getDurationInMS());
    Collection<Object> responses = receiveFromAllSlaves(10 * 1000/*sec wait*/);

    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics duration = new DescriptiveStatistics();
    for (Object obj : responses) {
      if (!(obj instanceof RawBenchmarkCommand.Response)
              || (obj instanceof RawBenchmarkCommand.Response
              && ((RawBenchmarkCommand.Response) obj).getPhase() != request.getPhase())) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        RawBenchmarkCommand.Response response = (RawBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getTotalSuccessfulOps());
        failedOps.addValue(response.getTotalFailedOps());
        speed.addValue(response.getOpsPerSec());
        duration.addValue(response.getRunTime());
      }
    }

    RawBMResults result = new RawBMResults(args.getNamenodeCount(),
            args.getNoOfNDBDataNodes(),
            request.getPhase(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))),
            (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()));
    printMasterResultMessages(result);
  }

  private void connectSlaves() throws IOException {
    if (args != null) {
      List<InetAddress> slaves = args.getListOfSlaves();
      for (InetAddress slave : slaves) {
        printMasterLogMessages("Connection to slave " + slave);
        try {
          SlaveConnection slaveConn = new SlaveConnection(slave, args.getSlaveListeningPort());
          slavesConnections.put(slave, slaveConn);
        } catch (Exception e) {
          misbehavingSlaves.add(slave);
          printMasterLogMessages("*** ERROR  unable to connect " + slave);
        }
      }
      if (misbehavingSlaves.size() >= args.getMaxSlavesFailureThreshold()) {
        printMasterLogMessages("*** Too many slaves failed. Abort test.");
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

  private void printMasterResultMessages(BMResults result) throws FileNotFoundException, IOException {
    blueColoredText(result.toString());
    FileWriter out = new FileWriter(args.getResultFile(), true);
    out.write(result.toString() + "\n");
    out.close();

    results.add(result);
  }

  private void resetResultFile() {
    File file = new File(args.getResultFile());
    if (file.exists()) {
      file.delete();
    }

    file = new File(args.getResultFile() + ConfigKeys.BINARY_RESULT_FILE_EXT);
    if (file.exists()) {
      file.delete();
    }
  }

  private void generateBinaryFile() throws FileNotFoundException, IOException {
    FileOutputStream fout = new FileOutputStream(args.getResultFile() + ConfigKeys.BINARY_RESULT_FILE_EXT);
    ObjectOutputStream oos = new ObjectOutputStream(fout);
    for (BMResults result : results) {
      oos.writeObject(result);
    }
    oos.close();
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
    BufferedReader br = new BufferedReader(new FileReader(args.getResultFile()));
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
          printMasterLogMessages("Sent " + obj.getClass().getName() + " to " + socket.getInetAddress());
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
          System.out.println("Setting time out to "+timeout);
          socket.setSoTimeout(timeout);
          ObjectInputStream recvFromSlave = new ObjectInputStream(socket.getInputStream());
          System.out.println("Goign to read obj "+timeout);
          Object obj = recvFromSlave.readObject();
          printMasterLogMessages("Recv " + obj.getClass().getName() + " from " + socket.getInetAddress());
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
      if (misbehavingSlaves.size() >= args.getMaxSlavesFailureThreshold()) {
        printMasterLogMessages("*** Too many slaves failed. Abort test.");
        System.exit(0);
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
