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

import io.hops.experiments.results.InterleavedBMResults;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author salman
 */
public class InterleavedBMResultsAggregator {

  private Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/> allResults =
          new HashMap<Integer, InterleavedAggregate>();

  public void processRecord(InterleavedBMResults result) {
    //System.out.println(result);

    InterleavedAggregate agg = allResults.get(result.getNoOfNamenodes());

    if (agg == null) {
      agg = new InterleavedAggregate();
      allResults.put(result.getNoOfNamenodes(), agg);
    }

    agg.addSpeed(result.getSpeed());
    agg.addFailedOps(result.getFailedOps());
    agg.addSucessfulOps(result.getSuccessfulOps());
    agg.addRunDuration(result.getDuration());
  }
  
  public Map<Integer, InterleavedAggregate> getResults(){
    return allResults;
  }
  
  public static void combineResults(Map<Integer, InterleavedAggregate> hdfs, Map<Integer, InterleavedAggregate> hopsfs, String outpuFolder) throws IOException {
    
    String data = "";
    String plot = "set terminal postscript eps enhanced color font \"Helvetica,14\"  #monochrome\n";
    plot += "set output '| ps2pdf - interleaved.pdf'\n";
    
    if(hdfs.keySet().size() > 1 ){
      System.out.println("NN count for HDFS cannot be greater than 1");
      return;
    }
    
    if(hopsfs.keySet().size() <= 0){
      return;
    }
    
    double hdfsVal = 0; 
    if(hdfs.keySet().size() == 1){
      hdfsVal = ((InterleavedAggregate)hdfs.values().toArray()[0]).getSpeed();
    }

    plot+="plot 'interleaved.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title \"HopsFS\" with errorbars, "+hdfsVal+" title \"HDFS\" \n";;
    
    for(Integer nn: hopsfs.keySet()){
      InterleavedAggregate agg = hopsfs.get(nn);
      data+=CompileResults.format(nn+"-NN")+CompileResults.format(agg.getSpeed()+"")+
              CompileResults.format(agg.getMinSpeed()+"")+CompileResults.format(agg.getMaxSpeed()+"")+
              "\n";
    }
    
    
    CompileResults.writeToFile(outpuFolder+"/interleaved.gnuplot", plot, false);
    CompileResults.writeToFile(outpuFolder+"/interleaved.dat", data, false);
  }
}
