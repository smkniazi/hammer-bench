/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional inCompileResults.formation regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.results.compiler;

import io.hops.experiments.benchmarks.common.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.rawthroughput.RawBMResults;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmarkCommand;
import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.utils.DFSOperationsUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class RawBMResultAggregator extends Aggregator {

  Map<Integer/*NN Count*/, Map<BenchmarkOperations, RawAggregate/*aggregates*/>> allResults =
          new HashMap<Integer, Map<BenchmarkOperations, RawAggregate>>();

  private static final String RECORD_NOT_FOUND = "-";

  public RawBMResultAggregator() {

  }

  @Override
  public void processRecord(BMResult result) {
    RawBMResults rResults = (RawBMResults) result;
    if (rResults.getSpeed() <= 0) {
      return;
    }

    Map<BenchmarkOperations, RawAggregate> map = allResults.get(rResults.getNoOfExpectedAliveNNs());

    if (map == null) {
      map = new HashMap<BenchmarkOperations, RawAggregate>();
      allResults.put(rResults.getNoOfExpectedAliveNNs(), map);
    }

    RawAggregate agg = map.get(rResults.getOperationType());
    if (agg == null) {
      agg = new RawAggregate();
      map.put(rResults.getOperationType(), agg);
    }

    agg.addSpeed(rResults.getSpeed());
    agg.addFailedOps(rResults.getFailedOps());
    agg.addSucessfulOps(rResults.getSuccessfulOps());
    agg.addRunDuration(rResults.getDuration());
  }

  @Override
  public boolean validate(BMResult result) {
    RawBMResults rResults = (RawBMResults) result;
    if (rResults.getSpeed() > 0 && rResults.getNoOfExpectedAliveNNs() == rResults.getNoOfAcutallAliveNNs()) {
      return true;
    }

    System.err.println("Inconsistent/Wrong results. " + rResults.getOperationType() + " Speed: " + rResults.getSpeed() +
            " Expected NNs: " + rResults.getNoOfExpectedAliveNNs() + " Actual NNs: " + rResults.getNoOfAcutallAliveNNs());
    return false;
  }

  CompiledResults processAllRecords() {
    CompiledResults cr = new CompiledResults();

    System.out.println("Generating compiled results for RAW Benchmarks");
    if (allResults.isEmpty()) {
      return cr;
    }

    SortedSet<Integer> sorted = new TreeSet<Integer>(); // Sort my number of NN
    sorted.addAll(allResults.keySet());
    for (Integer key : sorted) {
      cr.nnCounts.add(key);
      Map<BenchmarkOperations, RawAggregate> map = allResults.get(key);
      for (BenchmarkOperations op : map.keySet()) {
        RawAggregate agg = map.get(op);
        List<Double> avgVal = cr.avgVals.get(op);
        List<Double> maxVal = cr.maxVals.get(op);
        List<Double> minVal = cr.minVals.get(op);
        if (avgVal == null) {
          avgVal = new ArrayList<Double>();
          cr.avgVals.put(op, avgVal);
        }
        if (minVal == null) {
          minVal = new ArrayList<Double>();
          cr.minVals.put(op, minVal);
        }
        if (maxVal == null) {
          maxVal = new ArrayList<Double>();
          cr.maxVals.put(op, maxVal);
        }
        avgVal.add(agg.getSpeed());
        minVal.add(agg.getMinSpeed());
        maxVal.add(agg.getMaxSpeed());
      }
    }
    //create histogram
    for (BenchmarkOperations op : cr.avgVals.keySet()) {
      List<Double> vals = cr.avgVals.get(op);
      Double max = new Double(0);
      for (int i = 0; i < vals.size(); i++) {
        if (i > 0) {
          if (vals.get(i) < max) {
            vals.set(i, max);
          }
        }
        max = vals.get(i);
      }
    }

    //create histogram
    for (BenchmarkOperations op : cr.avgVals.keySet()) {
      List<Double> vals = cr.avgVals.get(op);
      List<Double> histo = cr.histoMap.get(op);
      if (histo == null) {
        histo = new ArrayList<Double>();
        cr.histoMap.put(op, histo);
      }
      Double previousVal = null;
      for (Double val : vals) {
        double diff = 0;
        if (previousVal == null) {
          previousVal = val;
          diff = val;
        } else {
          diff = (val - previousVal);
          if (diff < 0) {
            diff = 0;
          }
          previousVal = val;
        }
        histo.add(diff);
      }
    }

    return cr;
  }

  class CompiledResults {
    Map<BenchmarkOperations, List<Double>> avgVals = new HashMap<BenchmarkOperations, List<Double>>();
    Map<BenchmarkOperations, List<Double>> minVals = new HashMap<BenchmarkOperations, List<Double>>();
    Map<BenchmarkOperations, List<Double>> maxVals = new HashMap<BenchmarkOperations, List<Double>>();
    Map<BenchmarkOperations, List<Double>> histoMap = new HashMap<BenchmarkOperations, List<Double>>();
    List<Integer> nnCounts = new ArrayList<Integer>();
  }

  public static void combineHDFSandHopsFS(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException {
    if (!hdfsCr.nnCounts.isEmpty() && hdfsCr.nnCounts.size() > 1) {
      System.out.println("Invalid Results for HDFS. HDFS can not have more than one NN");
      return;
    }

    if (hopsFsCr.nnCounts.isEmpty()) {
      System.err.println("No HopsFS Results Found");
      return;
    }

//    if(hopsFsCr.nnCounts.get(0) != 1){
//      System.err.print("The first element in Hops Exeperiment should be 1");
//      return;
//    }

    lines(hdfsCr, hopsFsCr, outputFolder);
    histogram(hdfsCr, hopsFsCr, outputFolder);
    lineGraph(hdfsCr, hopsFsCr, outputFolder);

  }

  public static void lineGraph(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException {

    SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
    sorted.addAll(hopsFsCr.avgVals.keySet());
    List<Integer> NNIndex = hopsFsCr.nnCounts;
    for (BenchmarkOperations op : sorted) {
      String res = "";
      List<Double> avgList = hopsFsCr.avgVals.get(op);
      List<Double> minList = hopsFsCr.minVals.get(op);
      List<Double> maxList = hopsFsCr.maxVals.get(op);
      res += op.toString() + "\n";
      for (int i = 0; i < avgList.size(); i++) {
        res += CompileResults.format(NNIndex.get(i) + "") + "  " + CompileResults.format(minList.get(i) + "") + " " +
                CompileResults.format(avgList.get(i) + "") + " " +
                CompileResults.format(maxList.get(i) + "") + " " + "\n";
      }

      res += "\n";
      CompileResults.writeToFile(outputFolder + "/" + op + "-line.dat", res, false);
      System.out.println(res);
    }

  }


  private static void histogram(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException {

    String header = CompileResults.format("NameNodes");
    header += CompileResults.format("SingleNN");
    for (Integer i : hopsFsCr.nnCounts) {
      header += CompileResults.format(i + "-NN");
    }


    boolean isFirstRecord = true;
    String col = "t col";

    //TODO use String buffer everywhere
    String allData = "";
    String plotCommands = "";

    SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
    sorted.addAll(hopsFsCr.avgVals.keySet());
    for (BenchmarkOperations op : sorted) {
      int colorIndex = 0;
      List<Double> hopsHisto = hopsFsCr.histoMap.get(op);
      String msg = CompileResults.format("#" + op) + "\n";
      msg += header + "\n";
      msg += CompileResults.format("HopsFS");
      msg += CompileResults.format(RECORD_NOT_FOUND); // first col
      for (Double val : hopsHisto) {
        msg += CompileResults.format(DFSOperationsUtils.round(val) + "");
      }

      msg += "\n";
      msg += CompileResults.format("HDFS");
      List<Double> hdfsVals = hdfsCr.avgVals.get(op);
      if (hdfsVals != null) {
        if (hdfsVals.size() > 1) {
          System.err.println("In Hdfs there should be only one value for " + op);
          System.exit(0);
        }

        if (hdfsVals.size() == 1) {
          msg += CompileResults.format(hdfsVals.get(0) + "");
        }
      } else {
        msg += CompileResults.format(0 + "");
      }

      for (int i = 0; i < hopsFsCr.nnCounts.size(); i++) {
        msg += CompileResults.format(RECORD_NOT_FOUND);
      }
      msg += "\n";

      allData += msg;

      System.out.println(msg);
      CompileResults.writeToFile(outputFolder + "/" + op + ".dat", msg, false);

      String plotCommand = "";
      if (isFirstRecord) {
        plotCommand += "plot ";
      }

      plotCommand += " newhistogram \"" + op.toString().replace("_", "\\n") + "\", ";
      plotCommand += "\'" + op + ".dat\' ";
      plotCommand += " using \"SingleNN\":xtic(1) not  lc rgb '#d73027', ";
      for (Integer i : hopsFsCr.nnCounts) {
        plotCommand += "'' u \"" + i + "-NN\" " + col + getColor(colorIndex++) + " , ";
      }
      plotCommand += "\\\n";

      if (isFirstRecord) {
        isFirstRecord = false;
        col = "not";
      }


      plotCommands += plotCommand;


    }
    System.out.println(plotCommands);
    CompileResults.writeToFile(outputFolder + "/histo-internal.gnuplot", plotCommands, false);
    System.out.println(allData);
    CompileResults.writeToFile(outputFolder + "/histogram-all-data.dat", allData, false);
  }

  private static String getColor(int index) {
    //String[] colorMap = {"#8dd3c7", "#ffffb3", "#bebada", "#fb8072", "#80b1d3", "#fdb462", "#b3de69", "#fccde5", "#d9d9d9" };
    //String[] colorMap = {"#a6cee3", "#b2df8a", "#33a02c" , "#fb9a99", "#e31a1c", "#fdbf6f" , "#ff7f00", "#cab2d6", "#6a3d9a", "#ffff99", "#b15928" };
    // String[] colorMap = {"#d73027","#f46d43","#fdae61","#1a9850", "#66bd63", "#a6d96a"};
    String[] colorMap = {"#1a9850", "#66bd63", "#a6d96a"};
    //String[] colorMap = {"#d73027","#f46d43","#fdae61"};
    //String[] colorMap = {"#fef0d9","#fdd49e","#fdbb84","#fc8d59","#e34a33","#b30000"};
    return " lc rgb '" + colorMap[index % colorMap.length] + "' ";
  }


  private static void lines(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException {
    //  linear graph
    String outputFile = outputFolder + "/lines.txt";
    String allData = "";
    String header = CompileResults.format("NameNodes:");
    for (Integer i : hopsFsCr.nnCounts) {
      header += CompileResults.format(i + "-NN");
    }
    header += "\n";
    allData += header;


    SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
    sorted.addAll(hopsFsCr.avgVals.keySet());
    for (BenchmarkOperations op : sorted) {
      List<Double> hopsVals = hopsFsCr.avgVals.get(op);
      String msg = CompileResults.format("HopsFS_" + op.toString());
      for (Double val : hopsVals) {
        msg += CompileResults.format(DFSOperationsUtils.round(val) + "");
      }

      msg += "\n" + CompileResults.format("HDFS_" + op.toString());

      List<Double> hdfsVals = hdfsCr.avgVals.get(op);
      int hdfsRecordSize = 0;
      if (hdfsVals != null) {
        if (hdfsVals.size() > 1) {
          System.err.println("In Hdfs there should be only one value for " + op);
          System.exit(0);
        }

        if (hdfsVals.size() == 1) {
          msg += CompileResults.format(hdfsVals.get(0) + "");
        } else {
          msg += CompileResults.format(RECORD_NOT_FOUND);
        }
        hdfsRecordSize = 1;
      }

      for (int i = 0; i < hopsFsCr.nnCounts.size() - hdfsRecordSize; i++) {
        msg += CompileResults.format(RECORD_NOT_FOUND);
      }

      msg += "\n";
      allData += msg;
    }
    System.out.println(allData);
    CompileResults.writeToFile(outputFile, allData, false);

  }

  public static RawBMResults processSlaveResponses(Collection<Object> responses, RawBenchmarkCommand.Request request, BMConfiguration args) {
    DescriptiveStatistics successfulOps = new DescriptiveStatistics();
    DescriptiveStatistics failedOps = new DescriptiveStatistics();
    DescriptiveStatistics speed = new DescriptiveStatistics();
    DescriptiveStatistics duration = new DescriptiveStatistics();
    DescriptiveStatistics noOfAliveNNs = new DescriptiveStatistics();
    ArrayList<Long> latencies = new ArrayList<Long>();
    for (Object obj : responses) {
      if (!(obj instanceof RawBenchmarkCommand.Response)
              || (obj instanceof RawBenchmarkCommand.Response
              && ((RawBenchmarkCommand.Response) obj).getPhase() != request.getPhase())) {
        throw new IllegalStateException("Wrong response received from the client");
      } else {
        RawBenchmarkCommand.Response response = (RawBenchmarkCommand.Response) obj;
        successfulOps.addValue(response.getTotalSuccessfulOps());
        failedOps.addValue(response.getTotalFailedOps());
        speed.addValue(response.getOpsPerSec());
        duration.addValue(response.getRunTime());
        noOfAliveNNs.addValue(response.getNnCount());
        latencies.addAll(response.getOpsExeTimes());
      }
    }

    RawBMResults result = new RawBMResults(args.getNamenodeCount(),
            (int) Math.floor(noOfAliveNNs.getMean()),
            args.getNdbNodesCount(),
            request.getPhase(),
            (successfulOps.getSum() / ((duration.getMean() / 1000))),
            (duration.getMean() / 1000),
            (successfulOps.getSum()), (failedOps.getSum()), latencies);
    return result;
  }

}
