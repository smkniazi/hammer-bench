///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//        package io.hops.experiments.benchmarks.blockreporting;
//
//        import io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector;
//        import org.apache.commons.logging.Log;
//        import org.apache.commons.logging.LogFactory;
//        import org.apache.hadoop.hdfs.DFSConfigKeys;
//        import org.apache.hadoop.hdfs.protocol.Block;
//        import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
//        import org.apache.hadoop.hdfs.protocol.DatanodeID;
//        import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
//        import org.apache.hadoop.hdfs.server.datanode.DataNode;
//        import org.apache.hadoop.hdfs.server.datanode.DataStorage;
//        import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
//        import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
//        import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
//        import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
//        import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
//        import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
//        import org.apache.hadoop.hdfs.server.protocol.StorageReport;
//        import org.apache.hadoop.net.DNS;
//        import org.apache.hadoop.util.Time;
//        import org.apache.hadoop.util.VersionInfo;
//
//        import java.io.IOException;
//        import java.net.UnknownHostException;
//        import java.util.ArrayList;
//        import java.util.Collections;
//        import java.util.List;
//
//        import static io.hops.experiments.benchmarks.blockreporting.nn
//        .BlockReportingNameNodeSelector.BlockReportingNameNodeHandle;
//        import io.hops.experiments.controller.Logger;
//
//public class TinyDatanode implements Comparable<String> {
//
//  private static final long DF_CAPACITY = 100 * 1024 * 1024;
//  private static final long DF_USED = 0;
//
//  private static final Log LOG = LogFactory.getLog(TinyDatanode.class);
//
//  private final BlockReportingNameNodeSelector nameNodeSelector;
//
//  NamespaceInfo nsInfo;
//  DatanodeRegistration dnRegistration;
//  DatanodeStorage storage; //only one storage
//  ArrayList<Block> blocks;
//  int nrBlocks; // actual number of blocks
//  long[] blockReportList;
//  int dnIdx;
//
//  /**
//   * Return a a 6 digit integer port.
//   * This is necessary in order to provide lexocographic ordering.
//   * Host names are all the same, the ordering goes by port numbers.
//   */
//  private static int getNodePort(int num) throws IOException {
//    int port = 100000 + num;
//    if (String.valueOf(port).length() > 6) {
//      throw new IOException("Too many data-nodes");
//    }
//    return port;
//  }
//
//  TinyDatanode(BlockReportingNameNodeSelector nameNodeSelector, int dnIdx, int
//          blockCapacity) throws IOException {
//    this.dnIdx = dnIdx;
//    this.blocks = new ArrayList<Block>(
//            Collections.nCopies(blockCapacity, (Block) null));
//    this.nrBlocks = 0;
//    this.nameNodeSelector = nameNodeSelector;
//  }
//
//  @Override
//  public String toString() {
//    return dnRegistration.toString();
//  }
//
//  String getXferAddr() {
//    return dnRegistration.getXferAddr();
//  }
//
//  void register(boolean isDataNodePopulated) throws Exception {
//    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
//            .getNameNodes();
//    // get versions from the namenode
//    nsInfo = namenodes.get(0).getDataNodeRPC().versionRequest();
//    dnRegistration = new DatanodeRegistration(
//            new DatanodeID(DNS.getDefaultIP("default"),
//                    DNS.getDefaultHost("default", "default"),
//                    DataNode.generateUuid(), getNodePort(dnIdx),
//                    DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
//                    DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
//                    DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT),
//            new DataStorage(nsInfo), new ExportedBlockKeys(),
//            VersionInfo.getVersion());
//    //dnRegistration.setStorageID(createNewStorageId(dnRegistration.getXferPort(), dnIdx));
//    // register datanode
//    for(BlockReportingNameNodeHandle nn : namenodes) {
//      dnRegistration = nn.getDataNodeRPC().registerDatanode(dnRegistration);
//    }
//    //first block reports
//    storage = new DatanodeStorage(DatanodeStorage.generateUuid());
//    final StorageBlockReport[] reports = {
//            new StorageBlockReport(storage,
//                    new BlockListAsLongs(null, null).getBlockListAsLongs())
//    };
//    if(!isDataNodePopulated) {
//      firstBlockReport(reports);
//    }
//  }
//
//  /**
//   * Send a heartbeat to the name-node.
//   * Ignore reply commands.
//   */
//  void sendHeartbeat() throws Exception {
//    // register datanode
//    // TODO:FEDERATION currently a single block pool is supported
//    StorageReport[] rep = { new StorageReport(storage, false,
//            DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
//    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
//            .getNameNodes();
//    for(BlockReportingNameNodeHandle nn : namenodes) {
//      DatanodeCommand[] cmds = nn.getDataNodeRPC().sendHeartbeat(dnRegistration, rep,
//              0L, 0L, 0, 0, 0).getCommands();
//      if (cmds != null) {
//        for (DatanodeCommand cmd : cmds) {
//          if (LOG.isDebugEnabled()) {
//            LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
//          }
//        }
//      }
//    }
//  }
//
//  synchronized boolean addBlock(Block blk) {
//    if (nrBlocks == blocks.size()) {
//      if (LOG.isDebugEnabled()) {
//        LOG.debug("Cannot add block: datanode capacity = " + blocks.size());
//      }
//      return false;
//    }
//    blocks.set(nrBlocks, blk);
//    nrBlocks++;
//    return true;
//  }
//
//  void formBlockReport(boolean isDataNodePopulated) throws Exception {
//    // fill remaining slots with blocks that do not exist
//    for (int idx = blocks.size() - 1; idx >= nrBlocks; idx--) {
//      blocks.set(idx, new Block(Long.MAX_VALUE - (blocks.size() - idx), 0, 0));
//    }
//
//    blockReportList = new BlockListAsLongs(blocks, null).getBlockListAsLongs();
//
//    //first block report
////    if(isDataNodePopulated){
////      firstBlockReport(reports);
////    }
//
//    Logger.printMsg("Datanode # "+this.dnIdx+" has generated a block report of size "+blocks.size());
//  }
//
//  long[] blockReport() throws Exception {
//
//    final StorageBlockReport[] reports = {
//            new StorageBlockReport(storage, blockReportList)
//    };
//
//    long start1 = Time.now();
//    DatanodeProtocol nameNodeToReportTo = nameNodeSelector
//            .getNameNodeToReportTo(reports.length);
//
//    long start = Time.now();
//    blockReport(nameNodeToReportTo, reports);
//    long end = Time.now();
//    return new long[]{start - start1,  end - start};
//  }
//
//  private void firstBlockReport(StorageBlockReport[] reports) throws Exception {
//    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
//            .getNameNodes();
//    for(BlockReportingNameNodeHandle nn : namenodes){
//      blockReport(nn.getDataNodeRPC(), reports);
//    }
//  }
//
//  private void blockReport(DatanodeProtocol nameNodeToReportTo, StorageBlockReport[] reports) throws IOException {
//    nameNodeToReportTo.blockReport(dnRegistration, nsInfo.getBlockPoolID(),reports);
//  }
//
//  @Override
//  public int compareTo(String xferAddr) {
//    return getXferAddr().compareTo(xferAddr);
//  }
//
////  /**
////   * Return a a 6 digit integer port.
////   * This is necessary in order to provide lexocographic ordering.
////   * Host names are all the same, the ordering goes by port numbers.
////   */
////  private static int getNodePort(int num) throws IOException {
////    int port = 100000 + num;
////    if (String.valueOf(port).length() > 6) {
////      throw new IOException("Too many data-nodes");
////    }
////    return port;
////  }
////
////  TinyDatanode(BlockReportingNameNodeSelector nameNodeSelector, int dnIdx, int
////      blockCapacity) throws IOException {
////    this.dnIdx = dnIdx;
////    this.blocks = new ArrayList<Block>(
////        Collections.nCopies(blockCapacity, (Block) null));
////    this.nrBlocks = 0;
////    this.nameNodeSelector = nameNodeSelector;
////  }
////
////  @Override
////  public String toString() {
////    return dnRegistration.toString();
////  }
////
////  String getXferAddr() {
////    return dnRegistration.getXferAddr();
////  }
////
////  void register(boolean isDataNodePopulated) throws Exception {
////    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
////        .getNameNodes();
////    // get versions from the namenode
////    nsInfo = namenodes.get(0).getDataNodeRPC().versionRequest();
////    dnRegistration = new DatanodeRegistration(
////        new DatanodeID(DNS.getDefaultIP("default"),
////            DNS.getDefaultHost("default", "default"), "", getNodePort(dnIdx),
////            DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
////            DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT),
////        new DataStorage(nsInfo, ""), new ExportedBlockKeys(),
////        VersionInfo.getVersion());
////    dnRegistration.setStorageID(createNewStorageId(dnRegistration.getXferPort(), dnIdx));
////    // register datanode
////    for(BlockReportingNameNodeHandle nn : namenodes) {
////      dnRegistration = nn.getDataNodeRPC().registerDatanode(dnRegistration);
////    }
////    //first block reports
////    storage = new DatanodeStorage(dnRegistration.getStorageID());
////    if(!isDataNodePopulated) {
////      firstBlockReport(BlockReport.builder(1000).build());
////    }
////  }
////
////  /**
////   * Send a heartbeat to the name-node.
////   * Ignore reply commands.
////   */
////  void sendHeartbeat() throws Exception {
////    // register datanode
////    // TODO:FEDERATION currently a single block pool is supported
////    StorageReport[] rep =
////        {new StorageReport(dnRegistration.getStorageID(), false, DF_CAPACITY,
////            DF_USED, DF_CAPACITY - DF_USED, DF_USED)};
////
////    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
////        .getNameNodes();
////    for(BlockReportingNameNodeHandle nn : namenodes) {
////      DatanodeCommand[] cmds = nn.getDataNodeRPC().sendHeartbeat(dnRegistration, rep, 0, 0, 0)
////              .getCommands();
////      if (cmds != null) {
////        for (DatanodeCommand cmd : cmds) {
////          if (LOG.isDebugEnabled()) {
////            LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
////          }
////        }
////      }
////    }
////  }
////
////  synchronized boolean addBlock(Block blk) {
////    if (nrBlocks == blocks.size()) {
////      if (LOG.isDebugEnabled()) {
////        LOG.debug("Cannot add block: datanode capacity = " + blocks.size());
////      }
////      return false;
////    }
////    blocks.set(nrBlocks, blk);
////    nrBlocks++;
////    return true;
////  }
////
////  void formBlockReport(boolean isDataNodePopulated) throws Exception {
////    // fill remaining slots with blocks that do not exist
////    for (int idx = blocks.size() - 1; idx >= nrBlocks; idx--) {
////      blocks.set(idx, new Block(Long.MAX_VALUE - (blocks.size() - idx), 0, 0));
////    }
////    blockReportList = BlockReport.builder(1000).addAllAsFinalized(blocks)
////        .build();
////
////
////    //first block report
////    if(isDataNodePopulated){
////      firstBlockReport(blockReportList);
////    }
////
////    Logger.printMsg("Datanode # "+this.dnIdx+" has generated a block report of size "+blocks.size());
////  }
////
////  long[] blockReport() throws Exception {
////    return blockReport(blockReportList);
////  }
////
////  private long[] blockReport(BlockReport blocksReport) throws Exception {
////    long start1 = Time.now();
////    DatanodeProtocol nameNodeToReportTo = nameNodeSelector
////        .getNameNodeToReportTo(blocksReport.getNumBlocks());
////
////    long start = Time.now();
////    blockReport(nameNodeToReportTo, blocksReport);
////    long end = Time.now();
////    return new long[]{start - start1,  end - start};
////  }
////
////  private void firstBlockReport(BlockReport blocksReport) throws
////      Exception {
////    List<BlockReportingNameNodeHandle> namenodes = nameNodeSelector
////        .getNameNodes();
////    for(BlockReportingNameNodeHandle nn : namenodes){
////      blockReport(nn.getDataNodeRPC(), blocksReport);
////    }
////  }
////
////  private void blockReport(DatanodeProtocol nameNodeToReportTo,
////      BlockReport blocksReport) throws IOException {
////    StorageBlockReport[] report =
////        {new StorageBlockReport(storage, blocksReport)};
////    nameNodeToReportTo.blockReport(dnRegistration, nsInfo.getBlockPoolID(),
////        report);
////  }
////
////  @Override
////  public int compareTo(String xferAddr) {
////    return getXferAddr().compareTo(xferAddr);
////  }
////
//////  /**
//////   * Send a heartbeat to the name-node and replicate blocks if requested.
//////   */
//////  @SuppressWarnings("unused")
//////  // keep it for future blockReceived benchmark
//////  int replicateBlocks() throws IOException {
//////    // register datanode
//////    StorageReport[] rep =
//////        {new StorageReport(dnRegistration.getStorageID(), false, DF_CAPACITY,
//////            DF_USED, DF_CAPACITY - DF_USED, DF_USED)};
//////    DatanodeCommand[] cmds =
//////        datanodeProtocol.sendHeartbeat(dnRegistration, rep, 0, 0, 0)
//////            .getCommands();
//////    if (cmds != null) {
//////      for (DatanodeCommand cmd : cmds) {
//////        if (cmd.getAction() == DatanodeProtocol.DNA_TRANSFER) {
//////          // Send a copy of a block to another datanode
//////          BlockCommand bcmd = (BlockCommand) cmd;
//////          return transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
//////        }
//////      }
//////    }
//////    return 0;
//////  }
//////
//////  /**
//////   * Transfer blocks to another data-node.
//////   * Just report on behalf of the other data-node
//////   * that the blocks have been received.
//////   */
//////  private int transferBlocks(Block blocks[], DatanodeInfo xferTargets[][])
//////      throws IOException {
//////    for (int i = 0; i < blocks.length; i++) {
//////      DatanodeInfo blockTargets[] = xferTargets[i];
//////      for (int t = 0; t < blockTargets.length; t++) {
//////        DatanodeInfo dnInfo = blockTargets[t];
//////        DatanodeRegistration receivedDNReg;
//////        receivedDNReg = new DatanodeRegistration(dnInfo,
//////            new DataStorage(nsInfo, dnInfo.getStorageID()),
//////            new ExportedBlockKeys(), VersionInfo.getVersion());
//////        ReceivedDeletedBlockInfo[] rdBlocks =
//////            {new ReceivedDeletedBlockInfo(blocks[i],
//////                ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null)};
//////        StorageReceivedDeletedBlocks[] report =
//////            {new StorageReceivedDeletedBlocks(receivedDNReg.getStorageID(),
//////                rdBlocks)};
//////        datanodeProtocol.blockReceivedAndDeleted(receivedDNReg, nsInfo
//////            .getBlockPoolID(), report);
//////      }
//////    }
//////    return blocks.length;
//////  }
////
////
////  private static String createNewStorageId(int port,int dnIdx) {
////    // It is unlikely that we will create a non-unique storage ID
////    // for the following reasons:
////    // a) SecureRandom is a cryptographically strong random number generator
////    // b) IP addresses will likely differ on different hosts
////    // c) DataNode xfer ports will differ on the same host
////    // d) StorageIDs will likely be generated at different times (in ms)
////    // A conflict requires that all four conditions are violated.
////    // NB: The format of this string can be changed in the future without
////    // requiring that old SotrageIDs be updated.
////    String ip = "unknownIP";
////    try {
////      ip = DNS.getDefaultIP("default");
////    } catch (UnknownHostException ignored) {
////      LOG.warn("Could not find an IP address for the \"default\" inteface.");
////    }
////    return "DS-" + dnIdx + "-" + ip + "-" + port + "-" + dnIdx;
////  }
////
////}
