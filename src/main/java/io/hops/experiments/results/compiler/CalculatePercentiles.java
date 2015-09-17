/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.results.compiler;

import com.google.common.primitives.Doubles;
import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.ConfigKeys;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 *
 * @author salman
 */
public class CalculatePercentiles {

  public static void main(String argv[]) throws FileNotFoundException, IOException, ClassNotFoundException {
    new CalculatePercentiles().doShit(argv[0], argv[1], argv[2]);
  }
  
  private void doShit(String src, String dst, String prefix) throws FileNotFoundException, IOException, ClassNotFoundException {
    
    List<File> files = CompileResults.findFiles(src, ConfigKeys.RAW_RESPONSE_FILE_EXT);
    List<InterleavedBenchmarkCommand.Response> responses = new ArrayList<InterleavedBenchmarkCommand.Response>();
    
    for (File file : files) {
      FileInputStream fin = new FileInputStream(file);
      ObjectInputStream ois = new ObjectInputStream(fin);
      Object obj = null;
      try {
        while ((obj = ois.readObject()) != null) {
          if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
            System.out.println("Wrong binary file " + file);
            System.exit(0);
          } else {
            InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
            responses.add(response);
            System.out.println(response.getTotalSuccessfulOps());
          }
        }
      } catch (EOFException e) {
      } finally {
        ois.close();
      }
      
      
      
    }
    System.out.println("Starting to process raw data ");
    processResponses(responses, dst , prefix );
  }

  private void processResponses(List<InterleavedBenchmarkCommand.Response> responses, String path, String workloadName) throws IOException {
    Map<BenchmarkOperations, double[][]> allOpsPercentiles = new HashMap<BenchmarkOperations, double[][]>();
    Set<BenchmarkOperations> toProcess = new HashSet<BenchmarkOperations>();
    toProcess.add(BenchmarkOperations.CREATE_FILE);
    toProcess.add(BenchmarkOperations.READ_FILE);
    toProcess.add(BenchmarkOperations.LS_DIR);
    toProcess.add(BenchmarkOperations.DIR_INFO);

    //gather data for calculating percentiles
    Map<BenchmarkOperations, ArrayList<Long>> allOpsExecutionTimesList = new HashMap<BenchmarkOperations, ArrayList<Long>>();
    for (Object obj : responses) {
      InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
      HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes = response.getOpsExeTimes();
      for (BenchmarkOperations opType : opsExeTimes.keySet()) {
        if (toProcess.contains(opType)) {
          ArrayList<Long> opExeTimesFromSlave = opsExeTimes.get(opType);
          ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
          if (opAllExeTimes == null) {
            opAllExeTimes = new ArrayList<Long>();
            allOpsExecutionTimesList.put(opType, opAllExeTimes);
          }
          opAllExeTimes.addAll(opExeTimesFromSlave);
        }
      }
    }


    for (BenchmarkOperations opType : allOpsExecutionTimesList.keySet()) {
      if (toProcess.contains(opType)) {
        System.out.println("\n\nProcessing ...  " + opType);
        ArrayList<Long> opAllExeTimes = allOpsExecutionTimesList.get(opType);
        double[] toDouble = Doubles.toArray(opAllExeTimes);
        Percentile percentileCalculator = new Percentile();
        //percentileCalculator.setData(toDouble);
        double delta = 1;
        int rows = (int) Math.ceil((double) (100) / delta);
        double[][] percentile = new double[rows][2];
        int index = 0;
        for (double percen = delta; percen <= 100.0; percen += delta, index++) {
          percentile[index][0] = percentileCalculator.evaluate(toDouble, percen);
          percentile[index][1] = percen; // percentile
          System.out.println(opType + " Percentile " + percen + " Value: " + percentile[index][0]);
        }
        allOpsPercentiles.put(opType, percentile);

      }
    }
    generatePercentileGraphs(allOpsPercentiles, path, workloadName);
  }
  
   private void generatePercentileGraphs(Map<BenchmarkOperations, double[][]> allOpsPercentiles, String baseDir, String filesPrefix) throws IOException {
    String gnuplotFilePath = baseDir + "/" + filesPrefix + "-" + "percentiles.gnuplot";

    //generate dat files
    StringBuilder gnuplotFileTxt = new StringBuilder();
    gnuplotFileTxt.append("set terminal postscript eps enhanced color font \"Helvetica,12\"  #monochrome\n");
    gnuplotFileTxt.append("set output '| ps2pdf - " + filesPrefix + "-percentiles.pdf' \n");
    gnuplotFileTxt.append("#set key right bottom \n");
    gnuplotFileTxt.append("set xlabel \"Time (ms)\"\n");
    gnuplotFileTxt.append("#set ylabel \"Percentile\"\n");
    gnuplotFileTxt.append("#set yrange [0:1]\n\n\n");
    gnuplotFileTxt.append("plot ");

    StringBuilder dataFile = null;
    for (BenchmarkOperations opType : allOpsPercentiles.keySet()) {
      dataFile = new StringBuilder();
      String dataFilePath = baseDir + "/" + filesPrefix + "-" + opType + ".dat";
      double[][] data = allOpsPercentiles.get(opType);
      dataFile.append("0 0\n");
      for (int i = 0; i < data.length; i++) {
        dataFile.append(data[i][0]);
        dataFile.append(" ");
        dataFile.append(data[i][1]);
        dataFile.append("\n");
      }
      //System.out.println(dataFile.toString());
      CompileResults.writeToFile(dataFilePath, dataFile.toString(), false);
      gnuplotFileTxt.append(" \"").append(filesPrefix).append("-").append(opType).append(".dat").append("\" ");
      String title = opType.toString();
      title = title.replace("_", " ");
      gnuplotFileTxt.append(" using 1:2 title ").append("\"").append(title).append("\"");
      gnuplotFileTxt.append(" with lines  , \\\n");
    }

    //System.out.println(gnuplotFileTxt.toString());
    CompileResults.writeToFile(gnuplotFilePath, gnuplotFileTxt.toString(), false);

  }
   
}
