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
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author salman
 */
public class InterleavedBMResultsAggregator extends Aggregator{

  private Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/> allResults =
          new HashMap<Integer, InterleavedAggregate>();

  @Override
  public void processRecord(BMResult result) {
    //System.out.println(result);
    InterleavedBMResults ilResult = (InterleavedBMResults)result;

    InterleavedAggregate agg = allResults.get(ilResult.getNoOfNamenodes());

    if (agg == null) {
      agg = new InterleavedAggregate();
      allResults.put(ilResult.getNoOfNamenodes(), agg);
    }

    agg.addSpeed(ilResult.getSpeed());
    agg.addFailedOps(ilResult.getFailedOps());
    agg.addSucessfulOps(ilResult.getSuccessfulOps());
    agg.addRunDuration(ilResult.getDuration());
  }
  
  public Map<Integer, InterleavedAggregate> getResults(){
    return allResults;
  }
  
  public static void combineResults(Map<Integer, InterleavedAggregate> hdfs, Map<Integer, InterleavedAggregate> hopsfs, String outpuFolder) throws IOException {
    
    String data = "";
    String plot = "set terminal postscript eps enhanced color font \"Helvetica,18\"  #monochrome\n";
    plot += "set output '| ps2pdf - interleaved.pdf'\n";
    plot +=" set size 1,0.75 \n ";
    plot += "set ylabel \"ops/sec\" \n";
    plot += "set xlabel \"Number of Namenodes\" \n";
    plot += "set format y \"%.0s%c\"\n";
    
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
      data+=CompileResults.format(nn+"")+CompileResults.format(agg.getSpeed()+"")+
              CompileResults.format(agg.getMinSpeed()+"")+CompileResults.format(agg.getMaxSpeed()+"")+
              "\n";
    }
    
    
    CompileResults.writeToFile(outpuFolder+"/interleaved.gnuplot", plot, false);
    CompileResults.writeToFile(outpuFolder+"/interleaved.dat", data, false);
  }
}
