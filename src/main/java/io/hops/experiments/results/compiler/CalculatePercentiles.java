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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 *
 * @author salman
 */
public class CalculatePercentiles {

  private ExecutorService executor;
  public static void main(String argv[]) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
    new CalculatePercentiles().doShit(argv[0], argv[1], argv[2], Integer.parseInt(argv[3]));
  }
  
  private void doShit(String src, String dst, String prefix, int noOfThreads) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
    this.executor = Executors.newFixedThreadPool(noOfThreads);
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
            System.out.println("No of Records "+response.getTotalSuccessfulOps());
          }
        }
      } catch (EOFException e) {
      } finally {
        ois.close();
      }
      
      
      
    }
    System.out.println("Starting to process raw data ");
    processResponses(responses, dst , prefix );
    System.exit(0);
  }

  private void processResponses(List<InterleavedBenchmarkCommand.Response> responses, String path, String workloadName) throws IOException, InterruptedException {
    Map<BenchmarkOperations, Map<Double,Double>> allOpsPercentiles = new HashMap<BenchmarkOperations, Map<Double,Double>>();
    Set<BenchmarkOperations> toProcess = new HashSet<BenchmarkOperations>();
    toProcess.add(BenchmarkOperations.CREATE_FILE);
    toProcess.add(BenchmarkOperations.READ_FILE);
    toProcess.add(BenchmarkOperations.LS_DIR);
    toProcess.add(BenchmarkOperations.LS_FILE);
    toProcess.add(BenchmarkOperations.DIR_INFO);
    toProcess.add(BenchmarkOperations.FILE_INFO);

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
        List workers = new ArrayList<CalcPercentiles>();
        Map<Double,Double> percentileMap = new ConcurrentHashMap<Double,Double>();

        for (double percen = 10; percen <= 90.0; percen += 10) {
          workers.add(new CalcPercentiles(percentileMap, toDouble, percen));
        }

        for (double percen = 91; percen <= 99; percen += 1) {
          workers.add(new CalcPercentiles(percentileMap, toDouble, percen));
        }

        workers.add(new CalcPercentiles(percentileMap, toDouble, 99));

        for (double percen = 99.1; percen <= 100.0; percen += 0.1) {
          workers.add(new CalcPercentiles(percentileMap, toDouble, percen));
        }

        executor.invokeAll(workers); //block untill all points are calculated

        allOpsPercentiles.put(opType, percentileMap);

      }
    }
    generatePercentileGraphs(allOpsPercentiles, path, workloadName);
  }
  
   protected class CalcPercentiles implements Callable {

    final double[] data;
    final double point;
    final  Map<Double,Double> values;
    CalcPercentiles(Map<Double,Double> values, double[] data, double point){
      this.data = data;
      this.point = point;
      this.values= values;
    }
    
    @Override
    public Object call() throws Exception {
      Percentile p = new Percentile();
       double value = p.evaluate(data, point);
       if(values.get(point) == null){
         values.put(point, value);
         NumberFormat formatter = new DecimalFormat("#0.0");
         System.out.println(" Percentile " + formatter.format(point) + " Value: " + formatter.format(value)+" ns "+formatter.format(value/1000000.0)+" ms ");
       }else{
         throw new IllegalStateException("Dont calculate same data point twice");
       }
       return null;
    }
   }

  
   private void generatePercentileGraphs(Map<BenchmarkOperations, Map<Double,Double>> allOpsPercentiles, String baseDir, String filesPrefix) throws IOException {
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

    NumberFormat formatter = new DecimalFormat("#0.0");
    StringBuilder dataFile = null;
    for (BenchmarkOperations opType : allOpsPercentiles.keySet()) {
      dataFile = new StringBuilder();
      String dataFilePath = baseDir + "/" + filesPrefix + "-" + opType + ".dat";
      
      Map<Double,Double> percentileMap = allOpsPercentiles.get(opType);
      SortedSet<Double> sortedKeys = new TreeSet<Double>();
      sortedKeys.addAll(percentileMap.keySet());
      
      dataFile.append("#mano-sec micro-sec milli-sec key\n");
      dataFile.append("0 0 0 0\n");

      for(Double key: sortedKeys){
        Double value = percentileMap.get(key);
        dataFile.append(formatter.format(key));
        dataFile.append(" ");
        dataFile.append(formatter.format(value));
        dataFile.append(" ");
        dataFile.append(formatter.format(value/1000)); // micro
        dataFile.append(" ");
        dataFile.append(formatter.format(value/1000000)); // ms
        dataFile.append("\n");
      }
      
      //System.out.println(dataFile.toString());
      CompileResults.writeToFile(dataFilePath, dataFile.toString(), false);
      gnuplotFileTxt.append(" \"").append(filesPrefix).append("-").append(opType).append(".dat").append("\" ");
      String title = opType.toString();
      title = title.replace("_", " ");
      gnuplotFileTxt.append(" using 4:1 title ").append("\"").append(title).append("\"");
      gnuplotFileTxt.append(" with lines  , \\\n");
    }

    //System.out.println(gnuplotFileTxt.toString());
    CompileResults.writeToFile(gnuplotFilePath, gnuplotFileTxt.toString(), false);
  }
   
}
