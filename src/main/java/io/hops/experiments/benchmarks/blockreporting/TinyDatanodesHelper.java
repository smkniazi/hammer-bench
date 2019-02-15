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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TinyDatanodesHelper {
  /*
  CREATE TABLE `bench_blockreporting_datanodes` (
  `id` int(11) NOT NULL,
  `dn` int(11) NOT NULL,
  `data` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`,`dn`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
   */

  private static final String DATANODES_STATE = "/tmp/datanodes.state";

  private static final String SQL_INSERT = "INSERT IGNORE INTO " +
      "bench_blockreporting_datanodes (id, dn, data) values ('%d', '%d', '%s')";

  private static final String SQL_SELECT = "SELECT * from " +
      "bench_blockreporting_datanodes WHERE id<>'%d'";

  private final int slaveId;
  private final MysqlDataSource dataSource;

  private DatanodeInfo[] excludedDatanodes = null;

  public TinyDatanodesHelper(int slaveId, String databaseConnection) throws SQLException {
    String [] cnn = databaseConnection.split(":");
    this.slaveId = slaveId;
    dataSource = new MysqlDataSource();
    dataSource.setURL(databaseConnection);
  }


  public static void dropTable(String databaseConnection) throws SQLException {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setURL(databaseConnection);
    Connection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    System.out.println("Dropping table bench_blockreporting_datanodes");
    statement.executeUpdate("Drop table if exists  bench_blockreporting_datanodes");
    System.out.println("Dropped bench_blockreporting_datanodes table");
    statement.executeUpdate("CREATE TABLE `bench_blockreporting_datanodes` (" +
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

  public DatanodeInfo[] getExcludedDatanodes() throws SQLException {
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
    }
    return excludedDatanodes;
  }

  public void writeDataNodesStateToDisk(TinyDatanode[] datanodes) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(DATANODES_STATE));
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
    BufferedReader reader = new BufferedReader(new FileReader(DATANODES_STATE));
    String line = null;
    while ((line = reader.readLine()) != null){
      String[] rs = line.split(",");
      datanodes[Integer.valueOf(rs[0])].addBlock(new Block(Long.valueOf
          (rs[1]), Long.valueOf(rs[2]), Long.valueOf(rs[3])));
    }
    reader.close();
  }
}
