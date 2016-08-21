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
package io.hops.experiments.utils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import io.hops.experiments.workload.generator.FilePool;
import io.hops.experiments.workload.generator.FileTreeGenerator;
import io.hops.experiments.workload.generator.FixeDepthFileTreeGenerator;

public class BenchmarkUtils {

    private static ThreadLocal<FileSystem> dfsClients = new ThreadLocal<FileSystem>();
    private static ThreadLocal<FilePool> filePools = new ThreadLocal<FilePool>();
    
    private static int filePoolCount = 0;
    private static int dfsClientsCount = 0;

    public static FileSystem getDFSClient(Configuration conf) throws IOException {
        FileSystem client = dfsClients.get();
        if (client == null) {
            client = (FileSystem) FileSystem.newInstance(conf);
            dfsClients.set(client);
           System.out.println(Thread.currentThread().getName()  +
                " Creating new client. Total: "+ ++dfsClientsCount+" New Client is: "+client);
        }else{
            System.out.println("Reusing Existing Client "+client);
        }
        return client;
    }

    public static FilePool getFilePool(Configuration conf, String baseDir, 
            int dirsPerDir, int filesPerDir, boolean fixedDepthTree, int treeDepth) {
        FilePool filePool = filePools.get();
        if (filePool == null) {
            if(fixedDepthTree){
              filePool = new FixeDepthFileTreeGenerator(baseDir,treeDepth);
            }else{
              filePool = new FileTreeGenerator(baseDir,filesPerDir, dirsPerDir,0);
            }
            
            filePools.set(filePool);
            System.out.println("New FilePool created. Total :"+ ++filePoolCount);
        }else{
            //System.out.println("Reusing file pool obj");
        }
        
        return filePool;
    }
    
    public static void createFile(FileSystem dfs, Path path, short replication, final long size /*in bytes*/) throws IOException {
        FSDataOutputStream out = dfs.create(path, replication);
        if (size != 0) {
            for (long bytesWritten = 0; bytesWritten < size; bytesWritten += 4) {
                out.writeInt(1);
            }
        }
        out.close();
    }

    public static void readFile(FileSystem dfs, Path path, final long size /*in bytes*/) throws IOException {
        FSDataInputStream in = dfs.open(path);
        if (size != 0) {
            for (long bytesRead = 0; bytesRead < size; bytesRead += 4) {
                in.readInt();
            }
        }
        in.close();
    }

    public static boolean renameFile(FileSystem dfs, Path from, Path to) throws IOException {
        return dfs.rename(from, to);    
    }

    public static boolean deleteFile(FileSystem dfs, Path file) throws IOException {
        return dfs.delete(file, true);
    }
    
    public static void ls(FileSystem dfs, Path path) throws IOException {
       dfs.listStatus(path);
    }
    
    public static void getInfo(FileSystem dfs, Path path) throws IOException {
       dfs.getFileStatus(path);
    }
    
    public static void chmodPath(FileSystem dfs, Path path) throws IOException {
        dfs.setPermission(path, new FsPermission((short)0777));
    }
    
    public static void mkdirs(FileSystem dfs, Path path) throws IOException {
        dfs.mkdirs(path);
    }
    
    public static void chown(FileSystem dfs, Path path) throws IOException {     
        dfs.setOwner(path, System.getProperty("user.name"), System.getProperty("user.name"));
    }
    
    public static void setReplication(FileSystem dfs, Path path) throws IOException {
        dfs.setReplication(path, (short)3);
    }
    
    public static double round(double val){
      double round = val * 100;
      round = Math.ceil(round);
      return round / 100;
    }

  public static void appendFile(FileSystem dfs, Path path, long size) throws IOException {
    FSDataOutputStream out = dfs.append(path);
        if (size != 0) {
            for (long bytesWritten = 0; bytesWritten < size; bytesWritten += 4) {
                out.writeInt(1);
            }
        }
        out.close();
  }
}
