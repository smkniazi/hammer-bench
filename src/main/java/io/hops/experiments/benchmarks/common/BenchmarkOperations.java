/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.benchmarks.common;

/**
 *
 * @author salman
 */
public enum BenchmarkOperations {
    
    CREATE_FILE ("CREATE_FILE"),
    READ_FILE   ("READ_FILE"),
    RENAME_FILE ("RENAME_FILE"),
    DELETE_FILE ("DELETE_FILE"),
    STAT_DIR    ("STAT_DIR"),
    STAT_FILE   ("STAT_FILE"),
    CHMOD_FILE  ("CHMOD_FILE"),
    MKDIRS      ("MKDIR");

    private final String phase;
    private BenchmarkOperations(String phase){
        this.phase = phase;
    }
    
    public boolean equals(BenchmarkOperations otherName){
        return (otherName == null)? false:phase.equals(otherName.toString());
    }

    public String toString(){
       return phase;
    }
}

