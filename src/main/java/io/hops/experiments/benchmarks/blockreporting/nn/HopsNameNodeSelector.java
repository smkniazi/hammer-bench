/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.benchmarks.blockreporting.nn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hops.leader_election.node.ActiveNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NamenodeSelector;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

class HopsNameNodeSelector extends NamenodeSelector implements BlockReportingNameNodeSelector{

  private static class HopsNameNodeHandle extends NamenodeHandle implements
      BlockReportingNameNodeHandle{

    static Map<InetSocketAddress, DatanodeProtocol> datanodeProtocolMap =
        Maps.newHashMap();

    private DatanodeProtocol nameNodeDataNodeProto;

    HopsNameNodeHandle(NamenodeHandle handle, Configuration configuration)
        throws IOException {
      this(handle.getRPCHandle(), handle.getNamenode(), configuration);
    }

    HopsNameNodeHandle(ClientProtocol proto, ActiveNode an,
        Configuration configuration) throws IOException {
      super(proto, an);
      nameNodeDataNodeProto = datanodeProtocolMap.get(an
          .getInetSocketAddress());
      if(nameNodeDataNodeProto == null){
        nameNodeDataNodeProto = new DatanodeProtocolClientSideTranslatorPB(an
            .getInetSocketAddress(), configuration);
        datanodeProtocolMap.put(an.getInetSocketAddress(),
            nameNodeDataNodeProto);
      }
    }

    @Override
    public DatanodeProtocol getDataNodeRPC(){
      return nameNodeDataNodeProto;
    }
  }

  private Map<String, Integer> stats = Maps.newHashMap();

  HopsNameNodeSelector(Configuration conf, URI defaultUri)
      throws IOException {
    super(conf, defaultUri);
  }

  @Override
  public BlockReportingNameNodeHandle getNextNameNodeRPCS() throws IOException {
    NamenodeHandle namenodeHandle = getNextNamenode();
    return new HopsNameNodeHandle(namenodeHandle, conf);
  }

  @Override
  public BlockReportingNameNodeHandle getLeader() throws IOException {
    NamenodeHandle namenodeHandle = getLeadingNameNode();
    return new HopsNameNodeHandle(namenodeHandle, conf);
  }

  @Override
  public DatanodeProtocol getNameNodeToReportTo() throws
      IOException {
    BlockReportingNameNodeHandle leader = getLeader();
    NamenodeHandle namenodeHandle =getNamenodeHandle(leader.getDataNodeRPC
        ().getNextNamenodeToSendBlockReport().getInetSocketAddress());

    String nnip =namenodeHandle.getNamenode().getIpAddress();
    synchronized (stats){
      Integer v = stats.get(nnip);
      stats.put(nnip, v == null ? 0 : v+1);
    }

    return new HopsNameNodeHandle(namenodeHandle, conf).getDataNodeRPC();
  }

  @Override
  public List<BlockReportingNameNodeHandle> getNameNodes()
      throws IOException {
    List<NamenodeHandle> nns = getAllNameNode();
    List<BlockReportingNameNodeHandle> nameNodes =
        Lists.newArrayListWithExpectedSize(nns.size());
    for(NamenodeHandle nn : nns){
      nameNodes.add(new HopsNameNodeHandle(nn, conf));
    }
    return nameNodes;
  }

  @Override
  public Map<String, Integer> getReportsStats() {
    return stats;
  }
}
