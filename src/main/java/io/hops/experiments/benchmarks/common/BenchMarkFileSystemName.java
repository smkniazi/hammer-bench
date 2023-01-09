/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.benchmarks.common;

/**
 * @author salman
 */
public enum BenchMarkFileSystemName {
  HopsFS("HopsFS");

  private final String phase;

  private BenchMarkFileSystemName(String phase) {
    this.phase = phase;
  }

  public boolean equals(BenchmarkOperations otherName) {
    return (otherName == null) ? false : phase.equals(otherName.toString());
  }

  public String toString() {
    return phase;
  }

  public static BenchMarkFileSystemName fromString(String fsName) {
    if (fsName != null) {
      for (BenchMarkFileSystemName fsN : BenchMarkFileSystemName.values()) {
        if (fsN.toString().equals(fsName)) {
          return fsN;
        }
      }
    }
    return null;
  }
}
