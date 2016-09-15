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

import com.google.common.primitives.Doubles;
import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.ConfigKeys;
import io.hops.experiments.controller.MasterArgsReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class InterleavedBMResultsAggregator extends Aggregator {

  private Map<String /*workload name*/, Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/>> allWorkloadsResults =
          new HashMap<String, Map<Integer, InterleavedAggregate>>();

  @Override
  public void processRecord(BMResult result) {
    //System.out.println(result);
    InterleavedBMResults ilResult = (InterleavedBMResults) result;
    if(ilResult.getSpeed()<=0){
      return;
    }

    String workloadName = ilResult.getWorkloadName();
    Map<Integer, InterleavedAggregate> workloadResults = allWorkloadsResults.get(workloadName);
    if (workloadResults == null) {
      workloadResults = new HashMap<Integer, InterleavedAggregate>();
      allWorkloadsResults.put(workloadName, workloadResults);
    }

    InterleavedAggregate agg = workloadResults.get(ilResult.getNoOfExpectedAliveNNs());

    if (agg == null) {
      agg = new InterleavedAggregate();
      workloadResults.put(ilResult.getNoOfExpectedAliveNNs(), agg);
    }

    agg.addSpeed(ilResult.getSpeed());
    agg.addFailedOps(ilResult.getFailedOps());
    agg.addSucessfulOps(ilResult.getSuccessfulOps());
    agg.addRunDuration(ilResult.getDuration());
  }

  @Override
  public boolean validate(BMResult result) {
    InterleavedBMResults ilResult = (InterleavedBMResults) result;
    if (ilResult.getSpeed() > 0 && ilResult.getNoOfAcutallAliveNNs() == ilResult.getNoOfExpectedAliveNNs()) {
      return true;
    }
    System.err.println("Inconsistent/Wrong results.  Speed: "+ilResult.getSpeed()+
        " Expected NNs: "+ilResult.getNoOfExpectedAliveNNs()+" Actual NNs: "+ilResult.getNoOfAcutallAliveNNs());
    return false;
  }

  public Map<String, Map<Integer, InterleavedAggregate>> getResults() {
    return allWorkloadsResults;
  }

  public static void combineResults(Map<String, Map<Integer, InterleavedAggregate>> hdfsAllWorkLoads, Map<String, Map<Integer, InterleavedAggregate>> hopsfsAllWorkloas, String outpuFolder) throws IOException {

    String plot = "set terminal postscript eps enhanced color font \"Helvetica,18\"  #monochrome\n";
    plot +=  "set output '| ps2pdf - interleaved.pdf'\n";
    plot +=  "#set size 1,0.75 \n ";
    plot +=  "set ylabel \"ops/sec\" \n";
    plot +=  "set xlabel \"Number of Namenodes\" \n";
    plot +=  "set format y \"%.0s%c\"\n";
    plot +=  "plot ";

    for (String workload : hopsfsAllWorkloas.keySet()) {
      Map<Integer, InterleavedAggregate> hopsWorkloadResult = hopsfsAllWorkloas.get(workload);
      Map<Integer, InterleavedAggregate> hdfsWorkloadResult = hdfsAllWorkLoads.get(workload);

      if (hopsWorkloadResult == null) {
        System.out.println("No data for hopsfs for workload " + workload);
        return;
      }

      double hdfsVal = 0;
      if (hdfsWorkloadResult != null) {
        if (hdfsWorkloadResult.keySet().size() > 1) {
          System.out.println("NN count for HDFS cannot be greater than 1");
          return;
        }

        if (hdfsWorkloadResult.keySet().size() == 1) {
          hdfsVal = ((InterleavedAggregate) hdfsWorkloadResult.values().toArray()[0]).getSpeed();
        }
      }

      if (hopsWorkloadResult.keySet().size() <= 0) {
        return;
      }

      plot +=  " '" + workload + "-interleaved.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title \"HopsFS-" + workload + "\" with errorbars, " + hdfsVal + " title \"HDFS-" + workload + "\" \n";
      String data = "";
      SortedSet<Integer> sorted = new TreeSet<Integer>(); // Sort my number of NN
      sorted.addAll(hopsWorkloadResult.keySet());
      for (Integer nn : sorted) {
        InterleavedAggregate agg = hopsWorkloadResult.get(nn);
        data += CompileResults.format(nn + "") + CompileResults.format(agg.getSpeed() + "")
                + CompileResults.format(agg.getMinSpeed() + "") + CompileResults.format(agg.getMaxSpeed() + "")
                + "\n";
      }
      System.out.println(data);
      CompileResults.writeToFile(outpuFolder + "/" + workload + "-interleaved.dat", data, false);
    }

    System.out.println(plot);
    CompileResults.writeToFile(outpuFolder + "/interleaved.gnuplot", plot, false);
  }

  public static InterleavedBMResults processInterleavedResults(Collection<Object> responses, MasterArgsReader args) throws FileNotFoundException, IOException, InterruptedException {
    Map<BenchmarkOperations, double[][]> allOpsPercentiles = new HashMap<BenchmarkOperations, double[][]>();
    System.out.println("Processing the results ");
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics duration = new DescriptiveStatistics();
    DescriptiveStatistics opsLatency = new DescriptiveStatistics();
    DescriptiveStatistics noOfNNs = new DescriptiveStatistics();
    for (Object obj : responses) {
      if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getTotalSuccessfulOps());
        failedOps.addValue(response.getTotalFailedOps());
        speed.addValue(response.getOpsPerSec());
        duration.addValue(response.getRunTime());
        opsLatency.addValue(response.getAvgOpLatency());
        noOfNNs.addValue(response.getNnCount());
      }
    }
    
    //write the response objects to files. 
    //these files are processed by CalculatePercentiles.java
    int responseCount = 0;
    for (Object obj : responses) {
      if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        String filePath = args.getResultsDir();
        if (!filePath.endsWith("/")) {
          filePath += "/";
        }
        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
        filePath += "ResponseRawData"+responseCount+++ConfigKeys.RAW_RESPONSE_FILE_EXT;
        System.out.println("Writing Rwaw results to " + filePath);
        FileOutputStream fout = new FileOutputStream(filePath);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(response);
        oos.close();
      }
    }

    InterleavedBMResults result = new InterleavedBMResults(args.getNamenodeCount(),
            (int)Math.floor(noOfNNs.getMean()),
            args.getNdbNodesCount(), args.getInterleavedBmWorkloadName(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))), (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()), allOpsPercentiles, opsLatency.getMean());


    // failover testing
    if(args.testFailover()){
      if(responses.size() != 1){
        throw new UnsupportedOperationException("Currently we only support failover testing for one slave machine");
      }

      String prefix = args.getBenchMarkFileSystemName().toString();
      if(args.getBenchMarkFileSystemName() == BenchMarkFileSystemName.HopsFS){
        prefix+="-"+args.getNameNodeSelectorPolicy();
      }

      final String outputFolder = args.getResultsDir();
      InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response)responses.iterator().next();


      StringBuilder sb = new StringBuilder();
      for(String data : response.getFailOverLog()){
        sb.append(data).append("\n");
      }

      String datFile = prefix+"-failover.dat";
      CompileResults.writeToFile(outputFolder+"/"+datFile, sb.toString(), false);

      
      StringBuilder plot = new StringBuilder("set terminal postscript eps enhanced color font \"Helvetica,18\"  #monochrome\n");
      plot.append( "set output '| ps2pdf - failover.pdf'\n");
      plot.append( "#set size 1,0.75 \n ");
      plot.append( "set ylabel \"ops/sec\" \n");
      plot.append( "set xlabel \"Time (sec)\" \n");
      plot.append( "set format y \"%.0s%c\"\n");
      

      StringBuilder sbx = new StringBuilder();
      String oldPt = "";
      for(String data : response.getFailOverLog()){

        if(data.startsWith("#")) {
          StringTokenizer st = new StringTokenizer(oldPt);
          long time = Long.parseLong(st.nextToken());
          long spd = Long.parseLong(st.nextToken());
          sbx.append("set label 'NN-Restart' at "+time+","+spd+" rotate by 270").append("\n");
        }
        oldPt = data;
      }
      plot.append(sbx.toString());


      plot.append( "plot '"+datFile+"' with linespoints ls 1");
      CompileResults.writeToFile(outputFolder+"/"+prefix+"-failover.gnu", plot.toString(), false);
      
    }

    return result;
  }

 
}
