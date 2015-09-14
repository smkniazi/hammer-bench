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
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.MasterArgsReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

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
    
    System.out.println(plot);
    CompileResults.writeToFile(outpuFolder+"/interleaved.gnuplot", plot, false);
    System.out.println(data);
    CompileResults.writeToFile(outpuFolder+"/interleaved.dat", data, false);
  }
  
  public static InterleavedBMResults processInterleavedResults(Collection<Object> responses, MasterArgsReader args) throws FileNotFoundException, IOException {
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics duration = new DescriptiveStatistics();
    for (Object obj : responses) {
      if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getTotalSuccessfulOps());
        failedOps.addValue(response.getTotalFailedOps());
        speed.addValue(response.getOpsPerSec());
        duration.addValue(response.getRunTime());
      }
    }

    //gather data for calculating percentiles
    Map<BenchmarkOperations, double[][]> allOpsPercentiles = new HashMap<BenchmarkOperations, double[][]>();
    if (args.isPercentileEnabled()) {
      Map<BenchmarkOperations, ArrayList<Long>> allOpsExecutionTimesList = new HashMap<BenchmarkOperations, ArrayList<Long>>();
      for (Object obj : responses) {
        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
        HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes = response.getOpsExeTimes();
        for (BenchmarkOperations opType : opsExeTimes.keySet()) {
          ArrayList<Long> opExeTimesFromSlave = opsExeTimes.get(opType);
          ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
          if (opAllExeTimes == null) {
            opAllExeTimes = new ArrayList<Long>();
            allOpsExecutionTimesList.put(opType, opAllExeTimes);
          }
          opAllExeTimes.addAll(opExeTimesFromSlave);
        }
      }


      for (BenchmarkOperations opType : allOpsExecutionTimesList.keySet()) {
        ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
        double[] toDouble = Doubles.toArray(opAllExeTimes);
        Percentile percentileCalculator = new Percentile();
        percentileCalculator.setData(toDouble);
        double delta = 1;
        int rows = (int) Math.ceil((double) (100) / delta);
        double[][] percentile = new double[rows][2];
        int index = 0;
        for (double percen = delta; percen <= 100.0; percen += delta, index++) {  
          percentile[index][0] = percentileCalculator.evaluate(percen);
          percentile[index][1] = percen; // percentile
          //System.out.println("percent "+percen+" data "+percentile[index][0]);
        }
        allOpsPercentiles.put(opType, percentile);
      }
      
      generatePercentileGraphs(allOpsPercentiles, args.getResultsDir());
    }
    
    InterleavedBMResults result = new InterleavedBMResults(args.getNamenodeCount(),
            args.getNoOfNDBDataNodes(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))), (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()), allOpsPercentiles);
    return result;
  }
  
  private static void generatePercentileGraphs(Map<BenchmarkOperations, double[][]> allOpsPercentiles,String baseDir) throws IOException{
    String gnuplotFilePath = baseDir+"/percentiles.gnuplot";
    
    //generate dat files
    StringBuilder gnuplotFileTxt = new StringBuilder();
    gnuplotFileTxt.append("set terminal postscript eps enhanced color font \"Helvetica,12\"  #monochrome\n");
    gnuplotFileTxt.append("set output '| ps2pdf - percentiles.pdf' \n");
    gnuplotFileTxt.append("#set key right bottom \n");
    gnuplotFileTxt.append("set xlabel \"Time (ms)\"\n");
    gnuplotFileTxt.append("#set ylabel \"Percentile\"\n");
    gnuplotFileTxt.append("#set yrange [0:1]\n\n\n");
    gnuplotFileTxt.append("plot ");
    
    StringBuilder dataFile = null;
    for(BenchmarkOperations opType : allOpsPercentiles.keySet()){
      dataFile = new StringBuilder();
      String dataFilePath = baseDir+"/"+opType+".dat";
      double[][] data = allOpsPercentiles.get(opType);
      dataFile.append("0 0\n"); 
      for(int i = 0; i < data.length; i++){
        dataFile.append(data[i][0]);
        dataFile.append(" ");
        dataFile.append(data[i][1]);
        dataFile.append("\n");
      }
      //System.out.println(dataFile.toString());
      CompileResults.writeToFile(dataFilePath, dataFile.toString(), false);
      gnuplotFileTxt.append(" \"").append(opType).append(".dat").append("\" ");
      String title = opType.toString();
      title = title.replace("_", " ");
      gnuplotFileTxt.append(" using 1:2 title ").append("\"").append(title).append("\"");
      gnuplotFileTxt.append(" with lines  , \\\n");
    }
    
    //System.out.println(gnuplotFileTxt.toString());
    CompileResults.writeToFile(gnuplotFilePath, gnuplotFileTxt.toString(), false);
    
  }
  
  
}
