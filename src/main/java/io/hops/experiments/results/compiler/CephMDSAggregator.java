/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.results.compiler;


import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CephMDSAggregator {
  
  List<Integer> DATAPOINTS = Arrays.asList(1, 6, 12, 18, 24, 36, 48, 60);
  List<Integer> EXTENDED_DATAPOINTS = Arrays.asList(1, 6, 12, 18, 24, 30, 36,
      42, 48, 54, 60);
  
  List<String> EXPS = Arrays.asList("default", "pining", "skipKernel-pining");
  
  public static void main(String[] args) throws Exception {
    new CephMDSAggregator().processSpotify(args[0]);
  }
  
  
  void processSpotify(String baseDir) throws Exception {
    Map<String, Map<Integer, List<Double>>> allResults = new HashMap<String,
        Map<Integer, List<Double>>>();
  
    Map<String, Map<Integer, DescriptiveStatistics>> balance = new HashMap<String,
        Map<Integer, DescriptiveStatistics>>();
    
    for (String exp : EXPS) {
  
      Map<Integer, List<Double>> expResults = new HashMap<Integer,
          List<Double>>();
  
      Map<Integer, DescriptiveStatistics> expbalance= new HashMap<Integer,
          DescriptiveStatistics>();
      
      for (final int dp : DATAPOINTS) {
        File base = new File(baseDir, exp);
        File expDir = base.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.startsWith(dp +"-");
          }
        })[0];
        
        List<File> hopsResulsFiles = CompileResults.findFiles(expDir,
            ConfigKeys.BINARY_RESULT_FILE_NAME);
        if (hopsResulsFiles.size() != 1) {
          throw new RuntimeException(
              "There should be only one " + ConfigKeys.BINARY_RESULT_FILE_NAME);
        }
      
        ObjectInputStream ois =
            new ObjectInputStream(new FileInputStream(hopsResulsFiles.get(0)));
        InterleavedBMResults result = (InterleavedBMResults) ois.readObject();
      
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        List<File> files = CompileResults.findFiles(expDir, "-perf");
        Collections.sort(files, new Comparator<File>() {
          @Override
          public int compare(File o1, File o2) {
            return o1.getName().compareTo(o2.getName());
          }
        });
        
        for (File file : files) {
          JSONObject json = new JSONObject(FileUtils.readFileToString(file));
          if (json.has("mds_server")) {
            int requests = json.getJSONObject("mds_server").getInt(
                "handle_client_request");
            stats.addValue(requests);
          }
        }
        
        
        if (stats.getN() != dp) {
          throw new RuntimeException(
              "Number of active MDSs is incorrect, " + stats.getN() +
                  " Expected " + dp);
        }
      
        
        double mdsSpeed = stats.getSum() / result.getDuration();
        
        DescriptiveStatistics normalized = new DescriptiveStatistics();
        for(double val : stats.getValues()){
          normalized.addValue(val/stats.getSum());
        }
  
        double[] meanArr = new double[dp];
        Arrays.fill(meanArr, 1.0/dp);
        
        expResults.put(dp, Arrays.asList(result.getSpeed(), mdsSpeed,
            klDivergence(normalized.getValues(), meanArr), result.getAvgOpLatency()));
  
        expbalance.put(dp, normalized);
      }
      
      allResults.put(exp, expResults);
      balance.put(exp, expbalance);
    }
    
    
    StringBuilder sb = new StringBuilder();
    sb.append("#MDSS " + EXPS.toString() + "\n");
    sb.append("#MDSS expSpeed expSpeedPerMDS mdsSpeed permdsSpeed kldiv " +
        "avglatency \n");
    for (int dp : EXTENDED_DATAPOINTS) {
      sb.append(dp + " ");
      
      for(String exp : EXPS) {
        
        if(allResults.get(exp).containsKey(dp)) {
          List<Double> res = allResults.get(exp).get(dp);
          sb.append(res.get(0) + " " + (res.get(0)/dp)  + " "+ res.get(1)
              + " " + (res.get(1)/dp) + " " + res.get(2) + " " + (res.get(3)/1000000) + " ");
        }else{
          sb.append(" - - - - - -");
        }
      }
      sb.append("\n");
    }
    
    System.out.println(sb.toString());
    
    //show imbalance fo a a datapoint
    sb = new StringBuilder();
    
    final int dp = 24;
    String[] exps = new String[]{"default", "pining"};
    sb.append("#MDS default pining \n");
    for(int i=0; i<dp; i++){
      sb.append((i+1) + " " );
      for(String exp : exps){
        sb.append(balance.get(exp).get(dp).getValues()[i] + " ");
      }
      sb.append("\n");
    }
  
    System.out.println("\n" + sb.toString());
  }
  
  double klDivergence(double[] p1, double[] p2) {
    
    double div = 0.0;
    for (int i = 0; i < p1.length; ++i) {
      if (p1[i] == 0 || p2[i] == 0) { continue; }
      div += p1[i] * Math.log( p1[i] / p2[i] );
    }
    return div/ Math.log(2);
  }
  
}
