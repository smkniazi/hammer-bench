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
package io.hops.experiments.coin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;

/**
 *
 * @author salman
 */
public class MultiFaceCoin {
    
    private int create;
    private int read;
    private int rename;
    private int delete;
    private int statFile;
    private int statDir;
    private int chmodFiles;
    private int chmodDirs;
    private int mkdirs;
    private Random rand;
    private int expansion = 10;
    //1000 face dice
    ArrayList<BenchmarkOperations> dice = new ArrayList<BenchmarkOperations>();

    public MultiFaceCoin(int create, int read, int rename, int delete, int statFile, int statDir, int chmodFiles, int chmodDirs, int mkdirs) {
        this.create = create;
        this.read = read;
        this.rename = rename;
        this.delete = delete;
        this.chmodFiles = chmodFiles;
        this.chmodDirs = chmodDirs;
        this.mkdirs = mkdirs;
        this.statFile = statFile;
        this.statDir = statDir;
        this.rand = new Random(System.currentTimeMillis());
        
        createCoin();
    }

    private void createCoin(){
        
       System.out.println("Percentages create: "+create+" read: "+read+" mdir: "
               +mkdirs+" rename: "+rename+" delete: "+delete+" stat File: "
               +statFile+" statDir: "+statDir+" chmod files: "+chmodFiles+" chmod dirs: "+chmodDirs);
       int total = create+read+rename+delete+statFile+statDir+chmodFiles+chmodDirs+mkdirs;
       if(total != 100){
           throw new IllegalArgumentException("All probabilities should add to 100. Got: "+total);
       }

       for(int i = 0 ; i < create * expansion ; i++){
           dice.add(BenchmarkOperations.CREATE_FILE);
       }
       
       for(int i = 0 ; i < read * expansion ; i++){
           dice.add(BenchmarkOperations.READ_FILE);
       }
       
       for(int i = 0 ; i < rename * expansion ; i++){
           dice.add(BenchmarkOperations.RENAME_FILE);
       }
       
       for(int i = 0 ; i < delete * expansion ; i++){
           dice.add(BenchmarkOperations.DELETE_FILE);
       }
       
       for(int i = 0 ; i < statFile * expansion ; i++){
           dice.add(BenchmarkOperations.STAT_FILE);
       }
       
       for(int i = 0 ; i < statDir * expansion ; i++){
           dice.add(BenchmarkOperations.STAT_DIR);
       }
       
       for(int i = 0 ; i < chmodFiles * expansion ; i++){
           dice.add(BenchmarkOperations.CHMOD_FILE);
       }
       
       for(int i = 0 ; i < chmodDirs * expansion ; i++){
           dice.add(BenchmarkOperations.CHMOD_DIR);
       }
       
       for(int i = 0 ; i < mkdirs * expansion ; i++){
           dice.add(BenchmarkOperations.MKDIRS);
       }
       
       if (dice.size() != expansion * 100){
           throw new IllegalStateException("Dice is not properfly created");
       }
       
       Collections.shuffle(dice);
    }
    
    public BenchmarkOperations flip(){
        int choice = rand.nextInt(100* expansion);
        return dice.get(choice);
    }
}
