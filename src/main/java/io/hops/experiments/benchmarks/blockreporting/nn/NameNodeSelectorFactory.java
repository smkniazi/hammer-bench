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
//package io.hops.experiments.benchmarks.blockreporting.nn;
//
//import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
//import org.apache.hadoop.conf.Configuration;
//import java.net.URI;
//
//public class NameNodeSelectorFactory {
//  public static BlockReportingNameNodeSelector getSelector(
//          BenchMarkFileSystemName filesystemName, 
//          Configuration conf, URI defaultUri)
//      throws Exception {
//    
//    if(filesystemName == BenchMarkFileSystemName.HDFS){
//      return new HadoopNameNodeSelector(conf, defaultUri);
//    } if(filesystemName == BenchMarkFileSystemName.HopsFS){
//      return new HopsNameNodeSelector(conf, defaultUri);
//    } 
//    else{
//      throw new IllegalStateException("NameNode selection is only possible in HDFS and HopsFS");
//    }
//  }
//}
