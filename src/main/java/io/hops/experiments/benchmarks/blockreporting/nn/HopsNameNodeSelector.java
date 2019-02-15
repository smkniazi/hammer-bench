/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks.blockreporting.nn;

import com.google.common.collect.Maps;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.controller.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;

class HopsNameNodeSelector implements BlockReportingNameNodeSelector {

  private Map<String, Integer> stats = Maps.newHashMap();
  HopsNameNodesHandles hopsNameNodesHandles;

  HopsNameNodeSelector(Configuration conf, URI defaultUri) throws Exception {
    hopsNameNodesHandles = new HopsNameNodesHandles(conf, defaultUri);
  }

  @Override
  public BlockReportingNameNodeHandle getNextNameNodeRPCS() throws Exception {
    return hopsNameNodesHandles.getNextNameNodeRPCS();
  }

  @Override
  public BlockReportingNameNodeHandle getLeader() throws Exception {
    return hopsNameNodesHandles.getLeader();
  }

  @Override
  public DatanodeProtocol getNameNodeToReportTo(long blocksCount, DatanodeRegistration dnReg,
                                                boolean ignoreBRLoadBalancer) throws Exception {

    BlockReportingNameNodeHandle handle = hopsNameNodesHandles.getNameNodeToReportTo(blocksCount,
            dnReg, ignoreBRLoadBalancer);

    String nnip = handle.getHostName();
    synchronized (stats) {
      Integer v = stats.get(nnip);
      stats.put(nnip, v == null ? 1 : v + 1);
    }
    return handle.getDataNodeRPC();
  }

  @Override
  public List<BlockReportingNameNodeHandle> getNameNodes() throws Exception {
    return new ArrayList(hopsNameNodesHandles.getNamenodes());
  }

  @Override
  public Map<String, Integer> getReportsStats() {
    return stats;
  }
}
