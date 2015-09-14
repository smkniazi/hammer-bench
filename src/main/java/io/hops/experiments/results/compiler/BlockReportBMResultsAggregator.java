/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.results.compiler;

import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.blockreporting.BlockReportBMResults;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author salman
 */
public class BlockReportBMResultsAggregator extends Aggregator{
  
  private Map<Integer/*NN Count*/, BlockReportAggregate/*aggregates*/> allResults =
          new HashMap<Integer, BlockReportAggregate>();

  @Override
  public void processRecord(BMResult result){
    BlockReportBMResults brResult = (BlockReportBMResults)result;
    System.out.println(brResult);
    
    BlockReportAggregate agg = allResults.get(result.getNoOfNamenodes());

    if (agg == null) {
      agg = new BlockReportAggregate();
      allResults.put(brResult.getNoOfNamenodes(), agg);
    }

    agg.addSpeed(brResult.getSpeed());
    agg.addFailedOps(brResult.getFailedOps());
    agg.addSucessfulOps(brResult.getSuccessfulOps());
    agg.addRunDuration(-1);
    agg.addAvgTimePerPreport(brResult.getAvgTimePerReport());
    agg.addTimeToGetNameNodeToReport(brResult.getAvgTimeToGetNameNodeToReport());
  }
  
  public Map<Integer, BlockReportAggregate> getResults(){
    return allResults;
  }

  public static void combineResults(Map<Integer, BlockReportAggregate> hdfs, Map<Integer, BlockReportAggregate> hopsfs, String outpuFolder) throws IOException {
    String data = "";
    String plot = "set terminal postscript eps enhanced color font \"Helvetica,14\"  #monochrome\n";
    plot += "set output '| ps2pdf - block-report.pdf'\n";
    
    if(hdfs.keySet().size() > 1 ){
      System.out.println("NN count for HDFS cannot be greater than 1");
      return;
    }
    
    if(hopsfs.keySet().size() <= 0){
      return;
    }
    
        
    double hdfsVal = 0; 
    if(hdfs.keySet().size() == 1){
      hdfsVal = ((BlockReportAggregate)hdfs.values().toArray()[0]).getSpeed();
    }

    plot+="plot 'block-report.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title \"HopsFS\" with errorbars, "+hdfsVal+" title \"HDFS\" \n";;
    
    for(Integer nn: hopsfs.keySet()){
      BlockReportAggregate agg = hopsfs.get(nn);
      data+=CompileResults.format(nn+"-NN")+CompileResults.format(agg.getSpeed()+"")+
              CompileResults.format(agg.getMinSpeed()+"")+CompileResults.format(agg.getMaxSpeed()+"")+
              "\n";
    }
    
    
    System.out.println(plot);
    CompileResults.writeToFile(outpuFolder+"/block-report.gnuplot", plot, false);
    System.out.println(data);
    CompileResults.writeToFile(outpuFolder+"/block-report.dat", data, false);
    
  }
  
}
