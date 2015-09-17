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
import io.hops.experiments.controller.ConfigKeys;
import io.hops.experiments.controller.MasterArgsReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 *
 * @author salman
 */
public class InterleavedBMResultsAggregator extends Aggregator {

  private Map<String /*workload name*/, Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/>> allWorkloadsResults =
          new HashMap<String, Map<Integer, InterleavedAggregate>>();

//  private Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/> allResults =
//          new HashMap<Integer, InterleavedAggregate>();
  @Override
  public void processRecord(BMResult result) {
    //System.out.println(result);
    InterleavedBMResults ilResult = (InterleavedBMResults) result;

    String workloadName = ilResult.getWorkloadName();
    Map<Integer, InterleavedAggregate> workloadResults = allWorkloadsResults.get(workloadName);
    if (workloadResults == null) {
      workloadResults = new HashMap<Integer, InterleavedAggregate>();
      allWorkloadsResults.put(workloadName, workloadResults);
    }

    InterleavedAggregate agg = workloadResults.get(ilResult.getNoOfNamenodes());

    if (agg == null) {
      agg = new InterleavedAggregate();
      workloadResults.put(ilResult.getNoOfNamenodes(), agg);
    }

    agg.addSpeed(ilResult.getSpeed());
    agg.addFailedOps(ilResult.getFailedOps());
    agg.addSucessfulOps(ilResult.getSuccessfulOps());
    agg.addRunDuration(ilResult.getDuration());
  }

  public Map<String, Map<Integer, InterleavedAggregate>> getResults() {
    return allWorkloadsResults;
  }

  public static void combineResults(Map<String, Map<Integer, InterleavedAggregate>> hdfsAllWorkLoads, Map<String, Map<Integer, InterleavedAggregate>> hopsfsAllWorkloas, String outpuFolder) throws IOException {

    String plot = "set terminal postscript eps enhanced color font \"Helvetica,18\"  #monochrome\n";
    plot += "set output '| ps2pdf - interleaved.pdf'\n";
    plot += "#set size 1,0.75 \n ";
    plot += "set ylabel \"ops/sec\" \n";
    plot += "set xlabel \"Number of Namenodes\" \n";
    plot += "set format y \"%.0s%c\"\n";
    plot += "plot ";

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

      plot += " '" + workload + "-interleaved.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title \"HopsFS-" + workload + "\" with errorbars, " + hdfsVal + " title \"HDFS-" + workload + "\"  , \\\n";
      String data = "";
      for (Integer nn : hopsWorkloadResult.keySet()) {
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

  public static InterleavedBMResults processInterleavedResults(Collection<Object> responses, MasterArgsReader args) throws FileNotFoundException, IOException {
    Map<BenchmarkOperations, double[][]> allOpsPercentiles = new HashMap<BenchmarkOperations, double[][]>();
    System.out.println("Processing the results ");
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

//    Set<BenchmarkOperations> toProcess = new HashSet<BenchmarkOperations>();
//    toProcess.add(BenchmarkOperations.CREATE_FILE);
//    toProcess.add(BenchmarkOperations.READ_FILE);
//    toProcess.add(BenchmarkOperations.LS_DIR);
//    toProcess.add(BenchmarkOperations.DIR_INFO);
//
//    //gather data for calculating percentiles
//    
//    if (args.isPercentileEnabled()) {
//      Map<BenchmarkOperations, ArrayList<Long>> allOpsExecutionTimesList = new HashMap<BenchmarkOperations, ArrayList<Long>>();
//      for (Object obj : responses) {
//        InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
//        HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes = response.getOpsExeTimes();
//        for (BenchmarkOperations opType : opsExeTimes.keySet()) {
//          if (toProcess.contains(opType)) {
//            ArrayList<Long> opExeTimesFromSlave = opsExeTimes.get(opType);
//            ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
//            if (opAllExeTimes == null) {
//              opAllExeTimes = new ArrayList<Long>();
//              allOpsExecutionTimesList.put(opType, opAllExeTimes);
//            }
//            opAllExeTimes.addAll(opExeTimesFromSlave);
//          }
//        }
//      }
//
//
//      for (BenchmarkOperations opType : allOpsExecutionTimesList.keySet()) {
//        if (toProcess.contains(opType)) {
//          System.out.println("\n\nProcessing ...  " + opType);
//          ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
//          double[] toDouble = Doubles.toArray(opAllExeTimes);
//          Percentile percentileCalculator = new Percentile();
//          //percentileCalculator.setData(toDouble);
//          double delta = 1;
//          int rows = (int) Math.ceil((double) (100) / delta);
//          double[][] percentile = new double[rows][2];
//          int index = 0;
//          for (double percen = delta; percen <= 100.0; percen += delta, index++) {
//            percentile[index][0] = percentileCalculator.evaluate(toDouble, percen);
//            percentile[index][1] = percen; // percentile
//            System.out.println(opType + " Percentile " + percen + " Value: " + percentile[index][0]);
//          }
//          allOpsPercentiles.put(opType, percentile);
//
//        }
//      }
//      generatePercentileGraphs(allOpsPercentiles, args.getResultsDir(), args.getInterleavedWorkloadName());
//    }

    InterleavedBMResults result = new InterleavedBMResults(args.getNamenodeCount(),
            args.getNoOfNDBDataNodes(), args.getInterleavedWorkloadName(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))), (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()), allOpsPercentiles);
    return result;
  }

 
}
