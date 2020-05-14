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
package io.hops.experiments.benchmarks.blockreporting;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TinyDatanodesHelper {
  /*
  CREATE TABLE `bench_blockreporting_datanodes` (
  `id` int(11) NOT NULL,
  `dn` int(11) NOT NULL,
  `data` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`,`dn`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
   */

  private static final String SQL_INSERT = "INSERT IGNORE INTO " +
      "bench_blockreporting_datanodes (id, dn, data) values ('%d', '%d', '%s')";

  private static final String SQL_SELECT = "SELECT * from " +
      "bench_blockreporting_datanodes WHERE id<>'%d'";

  private final int slaveId;
  private final MysqlDataSource dataSource;
  private DatanodeInfo[] excludedDatanodes = null;
  private final BMConfiguration bmConf;

  public TinyDatanodesHelper(BMConfiguration bmConf, int slaveId) throws SQLException {
    this.slaveId = slaveId;
    this.bmConf = bmConf;
    dataSource = new MysqlDataSource();
    dataSource.setURL(bmConf.getBlockReportingPersistDatabase());
    createTable();
  }


  public void createTable() throws SQLException {
    Connection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
//    System.out.println("Dropping table bench_blockreporting_datanodes");
//    statement.executeUpdate("Drop table if exists  bench_blockreporting_datanodes");
//    System.out.println("Dropped bench_blockreporting_datanodes table");
    statement.executeUpdate("CREATE TABLE IF NOT EXISTS `bench_blockreporting_datanodes` (" +
            "  `id` int(11) NOT NULL, " +
            "  `dn` int(11) NOT NULL, " +
            "  `data` varchar(1000) NOT NULL, " +
            "  PRIMARY KEY (`id`,`dn`))");
    System.out.println("Created bench_blockreporting_datanodes table");
  }


  public void updateDatanodes(TinyDatanode[] datanodes) throws Exception {
    Connection connection = dataSource.getConnection();
    try {
      connection.setAutoCommit(false);
      Statement statement = connection.createStatement();
      for (TinyDatanode datanode : datanodes) {
        String info = datanode.dnRegistration.getIpAddr()   + "," +
                      datanode.dnRegistration.getHostName() + "," +
                      datanode.dnRegistration.getDatanodeUuid() + "," +
                      datanode.dnRegistration.getXferPort() + "," +
                      datanode.dnRegistration.getInfoPort() + "," +
                      datanode.dnRegistration.getInfoSecurePort() + "," +
                      datanode.dnRegistration.getIpcPort();

        statement.addBatch(String.format(SQL_INSERT, slaveId, datanode.dnIdx,
            info));
      }
      statement.executeBatch();
      connection.commit();
    } catch (Exception ex) {
      ex.printStackTrace();
      throw ex;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  public synchronized DatanodeInfo[] getExcludedDatanodes() throws SQLException {
    if (excludedDatanodes == null) {

      Connection connection = dataSource.getConnection();
      PreparedStatement statement = connection.prepareStatement(
          String.format(SQL_SELECT, slaveId));
      ResultSet resultSet = statement.executeQuery();
      List<DatanodeInfo> datanodeInfos = Lists.newArrayList();
      while (resultSet.next()) {
        String info = resultSet.getString("data");
        String[] dninfo = info.split(",");
        DatanodeID datanodeID = new DatanodeID(dninfo[0] /*ipAddress*/,
                dninfo[1] /*hostname*/,
                dninfo[2] /*DN UUID*/,
                Integer.valueOf(dninfo[3])/*xferPort*/,
                Integer.valueOf(dninfo[4])/*infoPort*/,
                Integer.valueOf(dninfo[5])/*infoSecurePort*/,
                Integer.valueOf(dninfo[6])/*ipcPort*/);

        datanodeInfos.add(new DatanodeInfo(datanodeID));
      }
      statement.close();
      excludedDatanodes = new DatanodeInfo[datanodeInfos.size()];
      excludedDatanodes = datanodeInfos.toArray(excludedDatanodes);
      System.out.println("Excluded Nodes Size "+ excludedDatanodes.length
              + " node(s) are excluded in this operation. "
              + Arrays.toString(excludedDatanodes));
    }
    return excludedDatanodes;
  }

  public void writeDataNodesStateToDisk(TinyDatanode[] datanodes,
                                        List<String> DNUUIDs,
                                        List<String> StorageUUIDs) throws IOException {
    GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(bmConf.brOnDiskStatePath())));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));

    //number of datanodes
    writer.write(Integer.toString(datanodes.length));
    writer.newLine();

    for(int i = 0; i < DNUUIDs.size(); i++){
      writer.write(DNUUIDs.get(i));
      writer.write(",");
      writer.write(StorageUUIDs.get(i));
      writer.newLine();
    }

    for(int dn=0; dn < datanodes.length; dn++){
      for(Block block : datanodes[dn].blocks){
        if(block != null) {
          writer.write(Joiner.on(",").join(dn, block.getBlockId(), block
              .getNumBytes(), block.getGenerationStamp()));
          writer.newLine();
        }
      }
    }
    writer.close();
  }

  public void readDataNodesStateFromDisk(TinyDatanode[] datanodes) throws IOException {
    GZIPInputStream zip = new GZIPInputStream(new FileInputStream(new File(bmConf.brOnDiskStatePath())));
    BufferedReader reader = new BufferedReader(new InputStreamReader(zip, "UTF-8"));
    String line = null;
    //ignore the count and UUIDs
    line = reader.readLine();
    for(int i = 0; i < Integer.parseInt(line); i++){
      reader.readLine(); //skip uuid
    }
    while ((line = reader.readLine()) != null){
      String[] rs = line.split(",");
      datanodes[Integer.valueOf(rs[0])].addBlock(new Block(Long.valueOf
          (rs[1]), Long.valueOf(rs[2]), Long.valueOf(rs[3]), ""));
    }
    reader.close();
  }

  public int getUUIDs(List<String> DNUUIDs, List<String> StorageUUIDs) throws IOException {
    GZIPInputStream zip = new GZIPInputStream(new FileInputStream(new File(bmConf.brOnDiskStatePath())));
    BufferedReader reader = new BufferedReader(new InputStreamReader(zip, "UTF-8"));
    String line = reader.readLine();
    int count = Integer.parseInt(line);
    for(int i = 0; i < count; i++){
      String IDs[] = reader.readLine().split(",");
      DNUUIDs.add(IDs[0]);
      StorageUUIDs.add(IDs[1]);
    }
    reader.close();
    return count;
  }
}
