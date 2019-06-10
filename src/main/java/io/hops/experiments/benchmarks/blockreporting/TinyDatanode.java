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
import io.hops.experiments.benchmarks.common.config.BMConfiguration;
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
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector.BlockReportingNameNodeHandle;

public class TinyDatanode implements Comparable<String> {

  private static final Log LOG = LogFactory.getLog(TinyDatanode.class);

  private static final long DF_CAPACITY = Long.MAX_VALUE;
  private static final long DF_USED = 0;


  private final BlockReportingNameNodeSelector nameNodeSelector;
  private final String machineName;
  private final AtomicInteger successfulBlksCreated = new AtomicInteger(0);
  private final AtomicInteger failedOps = new AtomicInteger(0);
  private final TinyDatanodesHelper helper;
  private final TinyDatanodes tinyDatanodes;
  private final BMConfiguration bmConf;
  private final String DNUUID;
  private final String storageUUID;
  private BlockReport blockReport;

  protected NamespaceInfo nsInfo;
  protected DatanodeRegistration dnRegistration;
  protected DatanodeStorage storage; //only one storage
  protected Queue<Block> blocks;
  protected final int dnIdx;

  private int getNodePort(int num) throws IOException {
    return 10000 + num;
  }

  TinyDatanode(BlockReportingNameNodeSelector nameNodeSelector, int dnIdx,
               TinyDatanodesHelper helper,
               TinyDatanodes tinyDatanodes, String DNUUID, String storageUUID,
               BMConfiguration bmConf)
          throws IOException {
    this.bmConf = bmConf;
    this.dnIdx = dnIdx;
    this.nameNodeSelector = nameNodeSelector;
    this.blocks = new ConcurrentLinkedQueue<Block>();
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
    StorageReport[] rep =
            {new StorageReport(storage, false, DF_CAPACITY,
                    DF_USED, DF_CAPACITY - DF_USED, DF_USED)};

    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
            .getNameNodes();
    for (BlockReportingNameNodeHandle nn : namenodes) {
      DatanodeCommand[] cmds = nn.getDataNodeRPC().sendHeartbeat(dnRegistration, rep, 0, 0, 0, 0,
              0, null)
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
    for (int idx = 0; idx < bmConf.getBRWarmupPhaseThreadsPerDN(); idx++) {
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
        if (!bmConf.getBaseDir().trim().endsWith("/")) {
          clientDir = bmConf.getBaseDir() + File.separator;
        } else {
          clientDir = bmConf.getBaseDir();
        }
        clientDir = clientDir + getClientName(tid);
        FileNameGenerator nameGenerator = new FileNameGenerator(clientDir, bmConf.getFilesPerDir());
        String clientName = getClientName(tid);

        while (successfulBlksCreated.get() < bmConf.getBlockReportingNumOfBlocksPerReport()) {
          try {

            String fileName = nameGenerator.getNextFileName("br");
            HdfsFileStatus status = nameNodeProto.create(fileName, FsPermission.getDefault(), clientName,
                    new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.CREATE)),
                    true, (short) bmConf.getReplicationFactor(), bmConf.getBlockReportingMaxBlockSize());
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
      } catch (Exception e) {
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
      for (int jdx = 0; jdx < bmConf.getBlockReportingNumOfBlocksPerFile(); jdx++) {
        LocatedBlock loc = null;
        try {
          loc = nameNodeProto.addBlock(fileName, clientName, prevBlock, helper.getExcludedDatanodes(),
                  fileID, new String[0]);
          prevBlock = loc.getBlock();
          prevBlock.setNumBytes(bmConf.getBlockReportingMaxBlockSize());
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
    BlockReport.Builder brBuilder = BlockReport.builder(bmConf.getNumBuckets());
    for (Block blk : blocks) {
      brBuilder.add(new FinalizedReplica(blk, null, null));
    }
    blockReport = brBuilder.build();

    //invliadate buckets
    for (int i = 0; i < bmConf.getBRNumInvalidBuckets(); i++){
      blockReport.getBuckets()[i].setHash(new byte[20]);
    }

    //do not send blocks of matching bucket to improve on the `wire` performance
    if(!bmConf.getBRIncludeBlocks()){
      for(int i = bmConf.getBRNumInvalidBuckets(); i < bmConf.getNumBuckets();i++){
        blockReport.getBuckets()[i].setBlocks(BlockListAsLongs.EMPTY);
      }
    }


    Logger.printMsg("Datanode # " + this.dnIdx + " has generated a block report of size " + blocks.size());

//    synchronized (nameNodeSelector) {
//      for (int i = 0; i < wReq.getNumBuckets(); i++) {
//        System.out.println("Datanode ID " + dnIdx + " Bucket ID " + i + " Hash " +
//                hashToString(blockReport.getBuckets()[i].getHash()));
//      }
//    }
//
//    for (Block blk : blocks) {
//      System.out.println(blk);
//    }

    //first block report
    if (bmConf.brReadStateFromDisk()) {
      firstBlockReport(blockReport);
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
            .getNameNodeToReportTo(blocksReport.getNumberOfBlocks(), dnRegistration,
                    bmConf.ignoreLoadBalancer());


    long start = Time.now();
    blockReport(nameNodeToReportTo, blocksReport);

    if (!bmConf.ignoreLoadBalancer()) {
      nameNodeToReportTo.blockReportCompleted(dnRegistration,  new DatanodeStorage[]{storage});
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

    long reportId = generateUniqueBlockReportId();
    nameNodeToReportTo.blockReport(dnRegistration, nsInfo.getBlockPoolID(),
            report, new BlockReportContext(1, 0, reportId));
  }

  long prevBlockReportId = 0;
  Random rand = new Random(System.currentTimeMillis());
  private long generateUniqueBlockReportId() {
    // Initialize the block report ID the first time through.
    // Note that 0 is used on the NN to indicate "uninitialized", so we should
    // not send a 0 value ourselves.
    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = rand.nextLong();
    }
    return prevBlockReportId;
  }

  @Override
  public int compareTo(String xferAddr) {
    return getXferAddr().compareTo(xferAddr);
  }
}
