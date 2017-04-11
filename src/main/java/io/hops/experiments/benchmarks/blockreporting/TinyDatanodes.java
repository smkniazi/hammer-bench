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
package io.hops.experiments.benchmarks.blockreporting;

import com.google.common.collect.Lists;
import io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector;
import io.hops.experiments.benchmarks.blockreporting.nn.NameNodeSelectorFactory;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.workload.generator.FileNameGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.EnumSetWritable;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector.BlockReportingNameNodeHandle;
import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.utils.BenchmarkUtils;
import java.util.concurrent.atomic.AtomicLong;

public class TinyDatanodes {

  private final BlockReportingNameNodeSelector nameNodeSelector;
  private final String baseDir;
  private final int nrDatanodes;
  private final int blocksPerReport;
  private final int blocksPerFile;
  private final int filesPerDirectory;
  private final short replication;
  private final int blockSize;
  private final TinyDatanode[] datanodes;
  private final String machineName;
  private final TinyDatanodesHelper helper;
  private AtomicLong filesCreated; //only for loggin
  private long filesToCreate;//only for loggin

  public TinyDatanodes(Configuration conf, String baseDir, int numOfDataNodes, int blocksPerReport, int blocksPerFile, int filesPerDirectory, int replication, int blockSize, int slaveId, String databaseConnection, BenchMarkFileSystemName fsName)
          throws IOException, Exception {
    this.baseDir = baseDir;
    this.nrDatanodes = numOfDataNodes;
    this.blocksPerReport = blocksPerReport;
    this.blocksPerFile = blocksPerFile;
    this.filesPerDirectory = filesPerDirectory;
    this.replication = (short) replication;
    this.blockSize = blockSize;
    this.datanodes = new TinyDatanode[nrDatanodes];
    conf.set(io.hops.experiments.controller.config.Configuration.ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, "ROUND_ROBIN");

    nameNodeSelector = NameNodeSelectorFactory.getSelector(fsName, conf, FileSystem
            .getDefaultUri(conf));
    machineName = InetAddress.getLocalHost().getHostName();
    this.helper = new TinyDatanodesHelper(slaveId, databaseConnection);
  }

  public void generateInput(boolean skipCreation, ExecutorService executor) throws
          Exception {
    int nrBlocks =
            (int) Math.ceil((double) blocksPerReport * nrDatanodes / replication);
    int nrFiles = (int) Math.ceil((double) nrBlocks / blocksPerFile);
    // create data-nodes
    String prevDNName = "";
    for (int idx = 0; idx < nrDatanodes; idx++) {
      System.out.println("register DN " + idx);
      datanodes[idx] = new TinyDatanode(nameNodeSelector, idx, blocksPerReport);
      datanodes[idx].register(skipCreation);
      assert datanodes[idx].getXferAddr().compareTo(prevDNName)
              > 0 : "Data-nodes must be sorted lexicographically.";
      datanodes[idx].sendHeartbeat();
      prevDNName = datanodes[idx].getXferAddr();
    }

    helper.updateDatanodes(datanodes);

    BlockReportingNameNodeHandle leader = nameNodeSelector.getLeader();

    leader.getRPCHandle().setSafeMode(
            HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);

    if (skipCreation) {
      helper.readDataNodesStateFromDisk(datanodes);
    } else {
      createFiles(nrFiles, executor);
    }
    // prepare block reports
    for (int idx = 0; idx < nrDatanodes; idx++) {
      datanodes[idx].formBlockReport(skipCreation);
    }
  }

  private void createFiles(int nrFiles, ExecutorService executor) throws
          Exception {
    filesCreated = new AtomicLong(0);
    filesToCreate = nrFiles;
    int fileCreationThreads = nrDatanodes * 100;

    Logger.printMsg(" Creating " + nrFiles + " files. Each file has "
            + blocksPerFile + " blocks.");

    List writers = Lists.newArrayList();

    int portion = (int) Math.floor(nrFiles / (double) fileCreationThreads);
    int curr = (nrFiles - (portion * fileCreationThreads)) + portion;
    for (int idx = 0; idx < fileCreationThreads; idx++) {
      BlockReportingNameNodeHandle nn = nameNodeSelector.getNextNameNodeRPCS();
      writers.add(new Writer(idx, curr, nn.getRPCHandle(),
              nn.getDataNodeRPC()));
      curr = portion;
    }

    Logger.printMsg("Going to start "+writers.size()+" worker for create files");
    executor.invokeAll(writers);

    helper.writeDataNodesStateToDisk(datanodes);
  }

  private class Writer implements Callable {

    private final int id;
    private final int nrFiles;
    private final ClientProtocol nameNodeProto;
    private final DatanodeProtocol datanodeProto;

    public Writer(int id, int nrFiles,
            ClientProtocol nameNodeProto,
            DatanodeProtocol datanodeProto) {
      this.id = id;
      this.nrFiles = nrFiles;
      this.nameNodeProto = nameNodeProto;
      this.datanodeProto = datanodeProto;
    }

    @Override
    public Object call() throws Exception {
      // create files
//      Logger.printMsg("Slave [" + id + "] creating  " + nrFiles + " files with "
//              + blocksPerFile + " blocks each.");
      String clientDir ="";
      if(!baseDir.trim().endsWith("/")){
        clientDir =  baseDir+File.separator;
      }else{
        clientDir = baseDir;
      }
      clientDir = clientDir + machineName+"_"+id+File.separator+id;
      FileNameGenerator nameGenerator = new FileNameGenerator(clientDir, filesPerDirectory);
      String clientName = getClientName(id);

      for (int idx = 0; idx < nrFiles; idx++) {
        try {
          String fileName = nameGenerator.getNextFileName("br");
          nameNodeProto.create(fileName, FsPermission.getDefault(), clientName,
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, replication,
              blockSize);
          ExtendedBlock lastBlock = addBlocks(nameNodeProto, datanodeProto, fileName, clientName);
          nameNodeProto.complete(fileName, clientName, lastBlock, null);
          filesCreated.incrementAndGet();
          log();
        } catch (Exception e){
          Logger.error(e);
        }
      }
      return null;
    }

    private String getClientName(int idx) {
      return "blockreporting-client-" + machineName + "_" + idx;
    }

    private void log() {
      if (Logger.canILog()) {
        double percent = ((double)filesCreated.get() / (double)filesToCreate) * 100;
        Logger.printMsg("Warmup " + BenchmarkUtils.round(percent) + "% completed");
      }
    }
  }

  private ExtendedBlock addBlocks(
          ClientProtocol nameNodeProto,
          DatanodeProtocol datanodeProto, 
          String fileName, String clientName)
          throws IOException, SQLException {
    ExtendedBlock prevBlock = null;
    for (int jdx = 0; jdx < blocksPerFile; jdx++) {
      LocatedBlock loc = null;
      try {
        loc = nameNodeProto.addBlock(fileName, clientName, prevBlock, helper.getExcludedDatanodes());
        prevBlock = loc.getBlock();
        for (DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());
          datanodes[dnIdx].addBlock(loc.getBlock().getLocalBlock());
          ReceivedDeletedBlockInfo[] rdBlocks = {new ReceivedDeletedBlockInfo(loc.getBlock().getLocalBlock(),
                  ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null)};
          StorageReceivedDeletedBlocks[] report = {new StorageReceivedDeletedBlocks(
                  datanodes[dnIdx].dnRegistration.getStorageID(), rdBlocks)};
          datanodeProto.blockReceivedAndDeleted(
                  datanodes[dnIdx].dnRegistration,
                  loc.getBlock().getBlockPoolId(), report);
        }
      }catch (IndexOutOfBoundsException e){
        System.out.println(e);
        System.out.println("Located block "+Arrays.toString(loc.getLocations()));
        System.out.println("Excluded Nodes are "+Arrays.toString(helper.getExcludedDatanodes()));
      }
    }
    return prevBlock;
  }

  long[] executeOp(int dnIdx)
          throws Exception {
    assert dnIdx < nrDatanodes : "Wrong dnIdx.";
    TinyDatanode dn = datanodes[dnIdx];
    return dn.blockReport();
  }



  void printStats() throws IOException {
    Logger.printMsg("Reports " + nameNodeSelector.getReportsStats().toString());
  }
}
