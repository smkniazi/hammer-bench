/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.benchmarks.e2eLatency;

import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkType;

/**
 *
 * @author salman
 */
public class E2ELatencyBMResult extends BMResult{
  public E2ELatencyBMResult(int noOfNameNodes, int noOfNDBDataNodes) {
    super(noOfNameNodes,noOfNDBDataNodes, BenchmarkType.E2ELatency);
  }
  @Override
  public String toString(){
    return "TODO: generate message";
  }
}
