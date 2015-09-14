/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.results.compiler;

import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 *
 * @author salman
 */
public class PercentileExample {
  public static void main(String[] argv){
    Percentile p = new Percentile();
    Random rand = new Random();
    int sampleSize = 10000000;
    double [] numbers = new double[sampleSize];
    
    for(int i = 0 ; i < sampleSize; i++){
      double randValue = rand.nextInt();
      //randValue = 1;
      numbers[i] = randValue;
    }

    p.setData(numbers);
    
    for(float i = 1; i <= 100; i+=0.5){
      System.out.println(i+" "+p.evaluate(i));
    }
  }
  
}
