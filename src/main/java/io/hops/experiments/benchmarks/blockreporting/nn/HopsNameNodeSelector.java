///**
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with this
// * work for additional information regarding copyright ownership. The ASF
// * licenses this file to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package io.hops.experiments.benchmarks.blockreporting.nn;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import io.hops.experiments.controller.ConfigKeys;
//import io.hops.experiments.controller.Logger;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hdfs.protocol.ClientProtocol;
//import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
//import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
//
//import java.io.IOException;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.net.InetSocketAddress;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hdfs.NameNodeProxies;
//import org.apache.hadoop.hdfs.server.namenode.NameNode;
//
//class HopsNameNodeSelector implements BlockReportingNameNodeSelector {
//
//  private class BlockReportingNameNodeHandleImpl implements BlockReportingNameNodeHandle {
//
//    ClientProtocol clientProto;
//    DatanodeProtocol datanodeProto;
//    String host;
//
//    public BlockReportingNameNodeHandleImpl(ClientProtocol clientProto, DatanodeProtocol datanodeProto, String host) {
//      this.clientProto = clientProto;
//      this.datanodeProto = datanodeProto;
//      this.host = host;
//    }
//
//    @Override
//    public ClientProtocol getRPCHandle() {
//      return clientProto;
//    }
//
//    @Override
//    public DatanodeProtocol getDataNodeRPC() {
//      return datanodeProto;
//    }
//
//    @Override
//    public String getHostName() {
//      return host;
//    }
//  };
//
//  private class HopsNameNodesHandles {
//
//    private Object namenodeSelector = null;
//    private final Map<InetSocketAddress, BlockReportingNameNodeHandle> protocolsMap = Maps.newHashMap();
//    private final ClientProtocol cp;
//    private final DatanodeProtocol dp;
//    private final Configuration config;
//
//    HopsNameNodesHandles(Configuration configuration, URI defaultUri) 
//            throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, IOException, InstantiationException {
//
//      NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
//              NameNodeProxies.createProxy(configuration, defaultUri, ClientProtocol.class);
//      config = configuration;
//      cp = proxyInfo.getProxy();
//      dp = new DatanodeProtocolClientSideTranslatorPB(NameNode.getAddress(configuration), configuration);
//      protocolsMap.put(NameNode.getAddress(configuration), new BlockReportingNameNodeHandleImpl(cp, dp, NameNode.getAddress(configuration).getAddress().getHostName().toString()));
//
//      try{
//      ClassLoader classLoader = HopsNameNodeSelector.class.getClassLoader();
//      Class nnSelectorClass = classLoader.loadClass("org.apache.hadoop.hdfs.NamenodeSelector");
//      Constructor constructor = nnSelectorClass.getConstructor(new Class[]{Configuration.class, URI.class});
//      namenodeSelector = constructor.newInstance(configuration, defaultUri);
//      System.out.println("NameNode Selector is initialized ");
//      }catch(ClassNotFoundException e){
//        Logger.printMsg("ERROR: Check the pom files and ensure that you have added correct dependencies for HopsFS. For testing HopsFS remove the HDFS dependencies from the pom file");
//        throw e;
//      }
//
//    }
//
//    private List<InetSocketAddress> getNameNodesInternal() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//      List<InetSocketAddress> rpcAddresses = new ArrayList<InetSocketAddress>();
//      Class nnSelectorClass = namenodeSelector.getClass();
//      Method method = nnSelectorClass.getMethod("getAllNameNode");
//      Object ret = method.invoke(namenodeSelector);
//      List<Object> handles = (List<Object>) ret;
//      for (Object handle : handles) {
//        rpcAddresses.add(getHandleIp(handle));
//      }
//      return rpcAddresses;
//    }
//
//    private InetSocketAddress getHandleIp(Object obj) throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//      Class cl = obj.getClass();
//      Method m = cl.getMethod("getNamenode");
//      Object ret = m.invoke(obj);
//
//      return (InetSocketAddress) getAnnIp(ret);
//    }
//    
//    private InetSocketAddress getAnnIp(Object obj) throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//      Class cl = obj.getClass();
//      Method m = cl.getMethod("getInetSocketAddress");
//      Object ret = m.invoke(obj);
//      return (InetSocketAddress) ret;
//      
//    }
//
//    public Collection<BlockReportingNameNodeHandle> getNameNodes() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
//      Class nnSelectorClass = namenodeSelector.getClass();
//      Method method = nnSelectorClass.getMethod("getAllNameNode");
//      Object ret = method.invoke(namenodeSelector);
//      List<Object> handles = (List<Object>) ret;
//      for (Object handle : handles) {
//        InetSocketAddress address = getHandleIp(handle);
//        getHandle(address); //also reates if it does not exist
//      }
//
//      return protocolsMap.values();
//    }
//
//    private BlockReportingNameNodeHandle getHandle(InetSocketAddress address) throws IOException {
//      if (!protocolsMap.containsKey(address)) {
//        System.out.println("Creating a handle for "+address.getAddress().getHostName()+":"+address.getPort());
//        config.set(ConfigKeys.FS_DEFAULTFS_KEY, "hdfs://" + address.getAddress().getHostName()+":"+address.getPort());
//        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
//                NameNodeProxies.createProxy(config, FileSystem.getDefaultUri(config), ClientProtocol.class);
//        ClientProtocol cpInt = proxyInfo.getProxy();
//        DatanodeProtocol dpInt = new DatanodeProtocolClientSideTranslatorPB(address, config);
//        
//        protocolsMap.put(address, new BlockReportingNameNodeHandleImpl(cpInt, dpInt, address.getAddress().getHostName()));
//      }
//      return protocolsMap.get(address);
//    }
//
//    public BlockReportingNameNodeHandle getNextNameNodeRPCS() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
//      Class nnSelectorClass = namenodeSelector.getClass();
//      Method method = nnSelectorClass.getMethod("getNextNamenode");
//      Object nnHandle = method.invoke(namenodeSelector);
//      InetSocketAddress address = getHandleIp(nnHandle);
//      return getHandle(address);
//    }
//
//    public BlockReportingNameNodeHandle getLeader() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
//      Class nnSelectorClass = namenodeSelector.getClass();
//      Method method = nnSelectorClass.getMethod("getLeadingNameNode");
//      Object nnHandle = method.invoke(namenodeSelector);
//      InetSocketAddress address = getHandleIp(nnHandle);
//      System.out.println("Leader is "+address);
//      return getHandle(address);
//    }
//
//    public BlockReportingNameNodeHandle getNameNodeToReportTo() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
//      BlockReportingNameNodeHandle leaderHandle =  getLeader();
//      DatanodeProtocol datanodeProto = leaderHandle.getDataNodeRPC();
//
//      Class dpClass = datanodeProto.getClass();
//      Method method = dpClass.getMethod("getNextNamenodeToSendBlockReport");
//      Object ann = method.invoke(datanodeProto);
//      InetSocketAddress address = getAnnIp(ann);
//      System.out.println("Sending BlockReport to "+address);
//      return getHandle(address);
//    }
//  }
//  private Map<String, Integer> stats = Maps.newHashMap();
//  HopsNameNodesHandles hopsNameNodesHandles;
//
//  HopsNameNodeSelector(Configuration conf, URI defaultUri)
//          throws Exception {
//    hopsNameNodesHandles = new HopsNameNodesHandles(conf, defaultUri);
//  }
//
//  @Override
//  public BlockReportingNameNodeHandle getNextNameNodeRPCS() throws Exception {
//    return hopsNameNodesHandles.getNextNameNodeRPCS();
//  }
//
//  @Override
//  public BlockReportingNameNodeHandle getLeader() throws Exception {
//    return hopsNameNodesHandles.getLeader();
//  }
//
//  @Override
//  public DatanodeProtocol getNameNodeToReportTo() throws Exception {
//
//    BlockReportingNameNodeHandle handle = hopsNameNodesHandles.getNameNodeToReportTo();
//
//    String nnip = handle.getHostName();
//    synchronized (stats) {
//      Integer v = stats.get(nnip);
//      stats.put(nnip, v == null ? 0 : v + 1);
//    }
//    return handle.getDataNodeRPC();
//  }
//
//  @Override
//  public List<BlockReportingNameNodeHandle> getNameNodes() throws Exception {
//    return new ArrayList(hopsNameNodesHandles.getNameNodes());
//  }
//
//  @Override
//  public Map<String, Integer> getReportsStats() {
//    return stats;
//  }
//}
