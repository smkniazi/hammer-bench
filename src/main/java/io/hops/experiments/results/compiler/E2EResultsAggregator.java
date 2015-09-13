/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.results.compiler;

import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.e2eLatency.E2ELatencyBMResult;

/**
 *
 * @author salman
 */
public class E2EResultsAggregator extends Aggregator {
  public void processRecord(E2ELatencyBMResult result){
    
  }

  @Override
  public void processRecord(BMResult result) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
