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

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.workload.generator.FileTreeFromDiskGenerator;
import net.smacke.jaydio.DirectRandomAccessFile;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import io.hops.experiments.workload.generator.FilePool;
import io.hops.experiments.workload.generator.FileTreeGenerator;
import io.hops.experiments.workload.generator.FixeDepthFileTreeGenerator;

public class DFSOperationsUtils {

    private static final boolean SERVER_LESS_MODE=false; //only for testing. If enabled then the clients will not
    private static Random rand = new Random(System.currentTimeMillis());
                                                        // contact NNs
    private static ThreadLocal<FileSystem> dfsClients = new ThreadLocal<FileSystem>();
    private static ThreadLocal<FilePool> filePools = new ThreadLocal<FilePool>();

    private static AtomicInteger filePoolCount = new AtomicInteger(0);
    private static AtomicInteger dfsClientsCount = new AtomicInteger(0);
    
    private static Boolean CEPH_USE_HADOOP_PLUGIN = null;
    private static Boolean CEPH_SKIP_KERNEL_CACHE = null;
    
    public static FileSystem getDFSClient(Configuration conf) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return null;
        }
    
        if(CEPH_USE_HADOOP_PLUGIN == null) {
            CEPH_USE_HADOOP_PLUGIN =
                conf.getBoolean(ConfigKeys.CEPH_USE_HADOOP_PLUGIN_KEY,
                    ConfigKeys.CEPH_USE_HADOOP_PLUGIN_DEFAULT);
            System.out.println("Ceph use hadoop plugin : " + CEPH_USE_HADOOP_PLUGIN);
        }
        
        if(CEPH_SKIP_KERNEL_CACHE == null) {
            CEPH_SKIP_KERNEL_CACHE =
                conf.getBoolean(ConfigKeys.CEPH_SKIP_KERNEL_CACHE_KEY,
                    ConfigKeys.CEPH_SKIP_KERNEL_CACHE_DEFAULT);
            System.out.println("Ceph skip Kernel Cache : " + CEPH_SKIP_KERNEL_CACHE);
        }
        
        
        if(CEPH_USE_HADOOP_PLUGIN) {
            FileSystem client = dfsClients.get();
            if (client == null) {
                client = (FileSystem) FileSystem.newInstance(conf);
                dfsClients.set(client);
                System.out.println(Thread.currentThread().getName() +
                    " Creating new client. Total: " +
                    dfsClientsCount.incrementAndGet() + " New Client is: " +
                    client);
            } else {
                System.out.println("Reusing Existing Client " + client);
            }
            return client;
        }else{
            return null;
        }
    }

    public static FilePool getFilePool(Configuration conf, String baseDir,
            int dirsPerDir, int filesPerDir, boolean fixedDepthTree, int treeDepth, String fileSizeDistribution,
                                       boolean readFilesFromDisk, String diskFilesPath) {
        FilePool filePool = filePools.get();
        if (filePool == null) {
            if(fixedDepthTree){
              filePool = new FixeDepthFileTreeGenerator(baseDir,treeDepth, fileSizeDistribution);
            }if(readFilesFromDisk){
              filePool = new FileTreeFromDiskGenerator(baseDir,filesPerDir, dirsPerDir,0, diskFilesPath);
            } else{
                filePool = new FileTreeGenerator(baseDir,filesPerDir, dirsPerDir,0, fileSizeDistribution);
            }
            
            filePools.set(filePool);
            System.out.println("New FilePool " +filePool+" created. Total :"+ filePoolCount.incrementAndGet());
        }else{
            System.out.println("Reusing file pool obj "+filePool);
        }
        return filePool;
    }
    
    public static void createFile(FileSystem dfs, String pathStr, short replication, FilePool filePool) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        
        if(CEPH_USE_HADOOP_PLUGIN) {
            FSDataOutputStream out = dfs.create(new Path(pathStr), replication);
            long size = filePool.getNewFileSize();
            if (size > 0) {
                byte[] buffer = new byte[64 * 1024];
                long read = -1;
                do {
                    read = filePool.getFileData(buffer);
                    if (read > 0) {
                        out.write(buffer, 0, (int) read);
                    }
                } while (read > -1);
            }
    
            out.close();
        }else{
            File file = new File(pathStr);
            File parentFile = file.getParentFile();
            if(!parentFile.exists()){
                parentFile.mkdirs();
            }
            long size = filePool.getNewFileSize();
            if(size > 0){
                FileOutputStream out = new FileOutputStream(file);
                byte[] buffer = new byte[64 * 1024];
                long read = -1;
                do {
                    read = filePool.getFileData(buffer);
                    if (read > 0) {
                        out.write(buffer, 0, (int) read);
                    }
                } while (read > -1);
                
            }else {
                file.createNewFile();
            }
        }
    }

    public static void readFile(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        
        if(CEPH_USE_HADOOP_PLUGIN) {
            FSDataInputStream in = dfs.open(new Path(pathStr));
            try {
                byte b;
                do {
                    b = in.readByte();
                } while (false);
            } catch (EOFException e) {
            } finally {
                in.close();
            }
        }else{
            if(CEPH_SKIP_KERNEL_CACHE) {
                DirectRandomAccessFile filereader =
                    new DirectRandomAccessFile(new File(pathStr), "r");
                if(filereader.length() > 0) {
                    byte[] data = new byte[(int) filereader.length()];
                    filereader.read(data);
                }
                filereader.close();
            }else {
                File file = new File(pathStr);
                FileInputStream filereader = new FileInputStream(file);
                if(file.length() > 0) {
                    byte[] data = new byte[(int) file.length()];
                    filereader.read(data);
                }
                filereader.close();
            }
    
        }
    }

    public static boolean renameFile(FileSystem dfs, Path from, Path to) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return true;
        }
        if(CEPH_USE_HADOOP_PLUGIN) {
            return dfs.rename(from, to);
        }else{
            File file = new File(from.toString());
            return file.renameTo(new File(to.toString()));
        }
    }

    public static boolean deleteFile(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return true;
        }
        if(CEPH_USE_HADOOP_PLUGIN) {
            return dfs.delete(new Path(pathStr), true);
        }else{
            File file = new File(pathStr);
            return file.delete();
        }
    }
    
    public static void ls(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        if(CEPH_USE_HADOOP_PLUGIN) {
            dfs.listStatus(new Path(pathStr));
        }else{
            File file = new File(pathStr);
            if(file.isDirectory()) {
                file.listFiles();
            }else{
                if(CEPH_SKIP_KERNEL_CACHE) {
                    DirectRandomAccessFile filereader =
                        new DirectRandomAccessFile(new File(pathStr), "r");
                    filereader.close();
                }else{
                    file.listFiles();
                }
            }
        }
    }
    
    public static void getInfo(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
       
        if(CEPH_USE_HADOOP_PLUGIN) {
            dfs.getFileStatus(new Path(pathStr));
        }else{
            File file = new File(pathStr);
            if(file.isDirectory()){
                file.lastModified();
            }else{
                if(CEPH_SKIP_KERNEL_CACHE) {
                    DirectRandomAccessFile filereader =
                        new DirectRandomAccessFile(new File(pathStr), "r");
                    filereader.length();
                    filereader.close();
                }else{
                    file.length();
                }
            }
        }
    }
    
    public static void chmodPath(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        if(CEPH_USE_HADOOP_PLUGIN) {
            dfs.setPermission(new Path(pathStr), new FsPermission((short)0777));
        }else{
            File file = new File(pathStr);
            file.setWritable(true);
            file.setExecutable(true);
            file.setReadable(true);
        }
    }
    
    public static void mkdirs(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        
        if(CEPH_USE_HADOOP_PLUGIN) {
            dfs.mkdirs(new Path(pathStr));
        }else{
            File dir = new File(pathStr);
            dir.mkdirs();
        }
    }
    
    public static void chown(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        
        //Not supported in Ceph
        //dfs.setOwner(new Path(pathStr), System.getProperty("user.name"),
         //   System.getProperty("user.name"));
    }
    
    public static void setReplication(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        //Not supported in Ceph
        //dfs.setReplication(new Path(pathStr), (short)3);
    }
    
    public static double round(double val){
      double round = val * 100;
      round = Math.ceil(round);
      return round / 100;
    }

    public static String format(int spaces, String string) {
        String format = "%1$-" + spaces + "s";
        return String.format(format, string);
    }

    public static boolean isTwoDecimalPlace(double val) {
        if (val == 0 || val == ((int) val)) {
            return true;
        } else {
            String valStr = Double.toString(val);
            int i = valStr.lastIndexOf('.');
            if (i != -1 && (valStr.substring(i + 1).length() == 1 || valStr.substring(i + 1).length() == 2)) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static void appendFile(FileSystem dfs, String pathStr, long size) throws IOException {
        if (SERVER_LESS_MODE) {
            serverLessModeRandomWait();
            return;
        }
    
        if(CEPH_USE_HADOOP_PLUGIN) {
            FSDataOutputStream out = dfs.append(new Path(pathStr));
            if (size != 0) {
                for (long bytesWritten = 0; bytesWritten < size;
                     bytesWritten += 1) {
                    out.writeByte(1);
                }
            }
            out.close();
        }else{
            if(CEPH_SKIP_KERNEL_CACHE) {
                DirectRandomAccessFile filereader =
                    new DirectRandomAccessFile(new File(pathStr), "rw");
                if(size != 0) {
                    byte[] buffer = new byte[(int) size];
                    Arrays.fill(buffer, (byte) 1);
                    filereader.write(buffer);
                }
                filereader.close();
            }else{
                FileOutputStream filereader = new FileOutputStream(new File(pathStr));
                if(size != 0) {
                    byte[] buffer = new byte[(int) size];
                    Arrays.fill(buffer, (byte) 1);
                    filereader.write(buffer);
                }
                filereader.close();
            }
        }
    }

    public static int getActiveNameNodesCount(BenchMarkFileSystemName fsName, FileSystem dfs) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (SERVER_LESS_MODE) {
            serverLessModeRandomWait();
            return 0;
        }

        //it only works for HopsFS
        if (fsName == BenchMarkFileSystemName.HopsFS) {
            Class filesystem = dfs.getClass();
            Method method = filesystem.getMethod("getNameNodesCount");
            Object ret = method.invoke(dfs);
            return (Integer) ret;
        } else if (fsName == BenchMarkFileSystemName.HDFS || fsName == BenchMarkFileSystemName.CephFS) {
            return 1;
        } else {
            throw new UnsupportedOperationException("Implement get namenode count for other filesystems");
        }
    }

    private static  void serverLessModeRandomWait(){
//        try {
//            Thread.sleep(rand.nextInt(100));
//            Thread.sleep(1);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
