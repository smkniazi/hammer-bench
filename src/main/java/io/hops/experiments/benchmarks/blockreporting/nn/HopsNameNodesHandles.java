package io.hops.experiments.benchmarks.blockreporting.nn;

import io.hops.experiments.benchmarks.blockreporting.nn.BlockReportingNameNodeSelector.BlockReportingNameNodeHandle;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingNonLeaderException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HopsNameNodesHandles {

  private class BlockReportingNameNodeHandleImpl implements BlockReportingNameNodeSelector.BlockReportingNameNodeHandle {

    ClientProtocol clientProto;
    DatanodeProtocol datanodeProto;
    String host;

    public BlockReportingNameNodeHandleImpl(ClientProtocol clientProto, DatanodeProtocol datanodeProto, String host) {
      this.clientProto = clientProto;
      this.datanodeProto = datanodeProto;
      this.host = host;
    }

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
  }

  private final Configuration config;
  private final URI defaultURI;
  private BlockReportingNameNodeHandle currentLeader;
  private Map<InetSocketAddress, BlockReportingNameNodeHandle> allHandles =
          new HashMap<InetSocketAddress, BlockReportingNameNodeHandle>();
  private Random rand = new Random(System.currentTimeMillis());
  private SortedActiveNodeList sanl ;

  HopsNameNodesHandles(Configuration configuration, URI defaultUri)
          throws IllegalArgumentException, IOException  {

    config = configuration;
    defaultURI = defaultUri;

    getAllNameNodes();
  }

  private void getAllNameNodes()
          throws IllegalArgumentException, IOException {
    NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
            NameNodeProxies.createProxy(config, defaultURI, ClientProtocol.class);

    ClientProtocol cp = proxyInfo.getProxy();
    sanl = cp.getActiveNamenodesForClient();

    for(ActiveNode an : sanl.getSortedActiveNodes()){
      BlockReportingNameNodeHandle brn = getHandle(an.getRpcServerAddressForClients());
      allHandles.put(an.getRpcServerAddressForClients(), brn);

       if(currentLeader == null){ //first one is the leader
         currentLeader = brn;
       }
    }
  }

  private BlockReportingNameNodeHandle getHandle(InetSocketAddress address) throws IOException {

      System.out.println("Creating a proxies for " +
              address.getAddress().getHostName() + ":" + address.getPort());

      config.set(ConfigKeys.FS_DEFAULTFS_KEY,
              "hdfs://" + address.getAddress().getHostName() + ":" + address.getPort());

      NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createProxy(
              config, FileSystem.getDefaultUri(config), ClientProtocol.class);
      ClientProtocol cp = proxyInfo.getProxy();
      DatanodeProtocol dp = new DatanodeProtocolClientSideTranslatorPB(address, config);
      return new BlockReportingNameNodeHandleImpl(cp, dp, address.getAddress().getHostName());
  }

  public synchronized BlockReportingNameNodeHandle getNextNameNodeRPCS()
          throws  IllegalArgumentException, IOException {
    return getRandomHandle();
  }

  public BlockReportingNameNodeHandle getNameNodeToReportTo(long blocksCount,
           DatanodeRegistration nodeReg, boolean ignoreBRLoadBalancer)
          throws  IllegalArgumentException, IOException {

    if(ignoreBRLoadBalancer){
      return getRandomHandle();
    } else {
      try {
        DatanodeProtocol datanodeProto = currentLeader.getDataNodeRPC();
        ActiveNode an = datanodeProto.getNextNamenodeToSendBlockReport(blocksCount, nodeReg);
        return allHandles.get(an.getRpcServerAddressForClients());
      } catch (BRLoadBalancingNonLeaderException e){
        //TODO
        throw e;
      }
    }
  }

  public  BlockReportingNameNodeHandle getLeader(){
    return currentLeader;
  }

  private BlockReportingNameNodeHandle getRandomHandle(){
    int index = rand.nextInt(allHandles.size());
    return (BlockReportingNameNodeHandle)allHandles.values().toArray()[index];
  }

  public Collection<BlockReportingNameNodeHandle> getNamenodes(){
    return allHandles.values();
  }

  public void closeAllHandles() {
    for(BlockReportingNameNodeHandle h : allHandles.values()){
      RPC.stopProxy(h.getDataNodeRPC());
      RPC.stopProxy(h.getRPCHandle());
      System.out.println("Closed Proxies for "+h.getHostName());
    }
  }

}

