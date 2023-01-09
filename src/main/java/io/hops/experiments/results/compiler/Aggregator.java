/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.results.compiler;

import io.hops.experiments.benchmarks.common.BMResult;

/**
 * @author salman
 */
public abstract class Aggregator {

  public abstract void processRecord(BMResult result);

  public abstract boolean validate(BMResult result);
}
