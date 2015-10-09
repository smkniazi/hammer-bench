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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

class HadoopNameNodeSelector implements BlockReportingNameNodeSelector{

  private final ClientProtocol clientProto;
  private final DatanodeProtocol datanodeProto;

  private BlockReportingNameNodeHandle nameNodeHandle = new
      BlockReportingNameNodeHandle() {
    @Override
    public ClientProtocol getRPCHandle() {
      return clientProto;
    }

    @Override
    public DatanodeProtocol getDataNodeRPC() {
      return datanodeProto;
    }

    @Override
    public String getHostName() {
      return host;
    }
  };

  private final List<BlockReportingNameNodeHandle> namenodes = Lists.newArrayList
      (nameNodeHandle);

  private int stats;
  private String host;
  HadoopNameNodeSelector(Configuration configuration, URI defaultUri)
      throws IOException {
    datanodeProto = new DatanodeProtocolClientSideTranslatorPB(
        NameNode.getAddress(configuration), configuration);
    NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
        NameNodeProxies.createProxy(configuration, defaultUri, ClientProtocol.class);
    clientProto = proxyInfo.getProxy();
    host = defaultUri.getHost();
  }

  @Override
  public BlockReportingNameNodeHandle getNextNameNodeRPCS()
      throws IOException {
    return nameNodeHandle;
  }

  @Override
  public BlockReportingNameNodeHandle getLeader()
      throws IOException {
    return nameNodeHandle;
  }

  @Override
  public DatanodeProtocol getNameNodeToReportTo() throws IOException {
    stats++;
    return datanodeProto;
  }

  @Override
  public List<BlockReportingNameNodeHandle> getNameNodes()
      throws IOException {
    return namenodes;
  }

  @Override
  public Map<String, Integer> getReportsStats() {
    Map<String, Integer> statsMap = Maps.newHashMap();
    statsMap.put(host, stats);
    return statsMap;
  }
}
