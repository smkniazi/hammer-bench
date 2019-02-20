/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.benchmarks.blockreporting;

import com.google.common.collect.Lists;
import io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.workload.generator.FileNameGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector.BlockReportingNameNodeHandle;

public class TinyDatanode implements Comparable<String> {

  private static final Log LOG = LogFactory.getLog(TinyDatanode.class);

  private static final long DF_CAPACITY = Long.MAX_VALUE;
  private static final long DF_USED = 0;


  private final BlockReportingNameNodeSelector nameNodeSelector;
  private final int threads;
  private final String machineName;
  private final AtomicInteger successfulBlksCreated = new AtomicInteger(0);
  private final AtomicInteger failedOps = new AtomicInteger(0);
  private final TinyDatanodesHelper helper;
  private final TinyDatanodes tinyDatanodes;
  private final BlockReportingWarmUp.Request wReq;
  private final String DNUUID;
  private final String storageUUID;
  private BlockReport blockReport;

  protected NamespaceInfo nsInfo;
  protected DatanodeRegistration dnRegistration;
  protected DatanodeStorage storage; //only one storage
  protected ArrayList<Block> blocks;
  protected final int dnIdx;

  //[S] why?

  /**
   * Return a a 5 digit integer port.
   * This is necessary in order to provide lexocographic ordering.
   * Host names are all the same, the ordering goes by port numbers.
   */
  private int getNodePort(int num) throws IOException {
    int port = 10000 + num;
    if (String.valueOf(port).length() > 5) {
      throw new IOException("Too many data-nodes");
    }
    return port;
  }

  //datanodes[idx] = new TinyDatanode(nameNodeSelector, idx,
//                                     5 /*threds for creation of blks*/, helper, this);
  TinyDatanode(BlockReportingNameNodeSelector nameNodeSelector, int dnIdx, int threads,
               TinyDatanodesHelper helper,
               TinyDatanodes tinyDatanodes, String DNUUID, String storageUUID,
               BlockReportingWarmUp.Request wReq)
          throws IOException {
    this.wReq = wReq;
    this.dnIdx = dnIdx;
    this.nameNodeSelector = nameNodeSelector;
    this.threads = threads;
    this.blocks = new ArrayList<Block>(wReq.getBlocksPerReport());
    this.machineName = InetAddress.getLocalHost().getHostName();
    this.helper = helper;
    this.tinyDatanodes = tinyDatanodes;
    this.DNUUID = DNUUID;
    this.storageUUID = storageUUID;
  }

  @Override
  public String toString() {
    return dnRegistration.toString();
  }

  String getXferAddr() {
    return dnRegistration.getXferAddr();
  }

  void register() throws Exception {
    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
            .getNameNodes();
    // get versions from the namenode
    nsInfo = namenodes.get(0).getDataNodeRPC().versionRequest();
    dnRegistration = new DatanodeRegistration(
            new DatanodeID(DNS.getDefaultIP("default"),
                    DNS.getDefaultHost("default", "default"),
                    DNUUID,
                    getNodePort(dnIdx),
                    DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT),
            new DataStorage(nsInfo), new ExportedBlockKeys(),
            VersionInfo.getVersion());

    // register datanode
    for (BlockReportingNameNodeHandle nn : namenodes) {
      dnRegistration = nn.getDataNodeRPC().registerDatanode(dnRegistration);
    }

    //first block reports
    storage = new DatanodeStorage(storageUUID);
//    if (wReq.brReadStateFromDisk()) {
//      BlockReport report = BlockReport.builder(wReq.getNumBuckets()).build();
//      firstBlockReport(report);
//    }
  }

  /**
   * Send a heartbeat to the name-node.
   * Ignore reply commands.
   */
  void sendHeartbeat() throws Exception {
    // register datanode
    // TODO:FEDERATION currently a single block pool is supported
    StorageReport[] rep =
            {new StorageReport(storage, false, DF_CAPACITY,
                    DF_USED, DF_CAPACITY - DF_USED, DF_USED)};

    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
            .getNameNodes();
    for (BlockReportingNameNodeHandle nn : namenodes) {
      DatanodeCommand[] cmds = nn.getDataNodeRPC().sendHeartbeat(dnRegistration, rep, 0, 0, 0, 0, 0)
              .getCommands();
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
          }
        }
      }
    }
  }

  public List createWriterThreads() throws Exception {
    List writers = Lists.newArrayList();
    for (int idx = 0; idx < threads; idx++) {
      writers.add(new Writer(idx, nameNodeSelector));
    }
    return writers;
  }

  private class Writer implements Callable {

    private final int tid;
    private final BlockReportingNameNodeSelector nameNodeSelector;
    private ClientProtocol nameNodeProto;
    private DatanodeProtocol datanodeProto;

    public Writer(int tid, BlockReportingNameNodeSelector nameNodeSelector) throws Exception {
      this.tid = tid;
      this.nameNodeSelector = nameNodeSelector;
    }

    @Override
    public Object call() throws Exception {

      try {
        BlockReportingNameNodeHandle nn = nameNodeSelector.getNextNameNodeRPCS();
        this.nameNodeProto = nn.getRPCHandle();
        this.datanodeProto = nn.getDataNodeRPC();

        String clientDir = "";
        if (!wReq.getBaseDir().trim().endsWith("/")) {
          clientDir = wReq.getBaseDir() + File.separator;
        } else {
          clientDir = wReq.getBaseDir();
        }
        clientDir = clientDir + getClientName(tid);
        FileNameGenerator nameGenerator = new FileNameGenerator(clientDir, wReq.getFilesPerDir());
        String clientName = getClientName(tid);

        while (successfulBlksCreated.get() < wReq.getBlocksPerReport()) {
          try {

            String fileName = nameGenerator.getNextFileName("br");
            System.out.println("creating file " + fileName);
            HdfsFileStatus status = nameNodeProto.create(fileName, FsPermission.getDefault(), clientName,
                    new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.CREATE)),
                    true, (short)wReq.getReplication(), wReq.getMaxBlockSize());
            ExtendedBlock lastBlock = addBlocks(nameNodeProto, datanodeProto, fileName, clientName,
                    status.getFileId());
            nameNodeProto.complete(fileName, clientName, lastBlock, status.getFileId(), null);
          } catch (IOException e) {
            failedOps.incrementAndGet();
            Logger.error(e);
          } finally {

          }
        }
        return null;
      }catch ( Exception e){
        Logger.error(e);
        throw e;
      }
    }

    private String getClientName(int idx) {
      return "br-client-" + machineName + "_" + dnRegistration.hashCode() + "_" + idx;
    }

    private ExtendedBlock addBlocks(ClientProtocol nameNodeProto,
                                    DatanodeProtocol datanodeProto,
                                    String fileName, String clientName,
                                    long fileID) throws IOException, SQLException {

      ExtendedBlock prevBlock = null;
      for (int jdx = 0; jdx < wReq.getBlocksPerFile(); jdx++) {
        LocatedBlock loc = null;
        try {
          loc = nameNodeProto.addBlock(fileName, clientName, prevBlock, helper.getExcludedDatanodes(),
                  fileID, new String[0]);
          prevBlock = loc.getBlock();
          for (DatanodeInfo dnInfo : loc.getLocations()) {

            int dnIdx = Arrays.binarySearch(tinyDatanodes.getAllDatanodes(), dnInfo.getXferAddr());
            tinyDatanodes.getAllDatanodes()[dnIdx].addBlock(loc.getBlock().getLocalBlock());
            ReceivedDeletedBlockInfo[] rdBlocks = {new ReceivedDeletedBlockInfo(loc.getBlock().getLocalBlock(),
                    ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null)};
            StorageReceivedDeletedBlocks[] report = {new StorageReceivedDeletedBlocks(
                    tinyDatanodes.getAllDatanodes()[dnIdx].storage.getStorageID(), rdBlocks)};
            datanodeProto.blockReceivedAndDeleted(tinyDatanodes.getAllDatanodes()[dnIdx].dnRegistration,
                    loc.getBlock().getBlockPoolId(), report);

            successfulBlksCreated.incrementAndGet();
            tinyDatanodes.incAllBlksCount();
            tinyDatanodes.log();
          }
        } catch (IndexOutOfBoundsException e) {
          System.out.println(e);
          System.out.println("Located block " + Arrays.toString(loc.getLocations()));
          System.out.println("Excluded Nodes are " + Arrays.toString(helper.getExcludedDatanodes()));
        }
      }
      return prevBlock;
    }
  }

  synchronized void addBlock(Block blk) {
    blocks.add(blk);
  }

  void formBlockReport() throws Exception {
    blockReport = BlockReport.builder(wReq.getNumBuckets()).addAllAsFinalized(blocks).build();

    synchronized (nameNodeSelector) {
      for (int i = 0; i < wReq.getNumBuckets(); i++) {
        System.out.println("Datanode ID " + dnIdx + " Bucket ID " + i + " Hash " +
                hashToString(blockReport.getBuckets()[i].getHash()));
      }
    }

      //first block report
    if (wReq.brReadStateFromDisk()) {
      firstBlockReport(blockReport);
    }

    Logger.printMsg("Datanode # " + this.dnIdx + " has generated a block report of size " + blocks.size());
    for (Block blk : blocks) {
      System.out.println(blk);
    }
  }

  public static String hashToString(byte[] hash) {
    StringBuilder sb = new StringBuilder();
    for (byte b : hash) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  long[] blockReport() throws Exception {
    return blockReport(blockReport);
  }

  private long[] blockReport(BlockReport blocksReport) throws Exception {
    long start1 = Time.now();
    DatanodeProtocol nameNodeToReportTo = nameNodeSelector
            .getNameNodeToReportTo(blocksReport.getNumberOfBlocks(), dnRegistration, wReq.ignoreBRLoadBalancer());


    long start = Time.now();
    blockReport(nameNodeToReportTo, blocksReport);

    if (!wReq.ignoreBRLoadBalancer()) {
      nameNodeToReportTo.blockReportCompleted(dnRegistration);
    }

    long end = Time.now();
    return new long[]{start - start1, end - start};
  }

  private void firstBlockReport(BlockReport blocksReport) throws
          Exception {
    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
            .getNameNodes();
    for (BlockReportingNameNodeHandle nn : namenodes) {
      blockReport(nn.getDataNodeRPC(), blocksReport);
    }
  }

  private void blockReport(DatanodeProtocol nameNodeToReportTo,
                           BlockReport blocksReport) throws IOException {
    StorageBlockReport[] report =
            {new StorageBlockReport(storage, blocksReport)};
    nameNodeToReportTo.blockReport(dnRegistration, nsInfo.getBlockPoolID(),
            report);
  }

  @Override
  public int compareTo(String xferAddr) {
    return getXferAddr().compareTo(xferAddr);
  }
}
