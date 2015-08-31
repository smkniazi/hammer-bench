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
import java.math.BigDecimal;

/**
 *
 * @author salman
 */
public class MultiFaceCoin {
    
    private BigDecimal create;
    private BigDecimal read;
    private BigDecimal rename;
    private BigDecimal delete;
    private BigDecimal lsFile;
    private BigDecimal lsDir;
    private BigDecimal chmodFiles;
    private BigDecimal chmodDirs;
    private BigDecimal mkdirs;
    private BigDecimal setReplication;
    private BigDecimal fileInfo;
    private BigDecimal dirInfo;
    //private BigDecimal 
    private Random rand;
    private int expansion = 10;
    //1000 face dice
    ArrayList<BenchmarkOperations> dice = new ArrayList<BenchmarkOperations>();

    public MultiFaceCoin(BigDecimal create, BigDecimal read, BigDecimal rename, BigDecimal delete, BigDecimal lsFile, 
            BigDecimal lsDir, BigDecimal chmodFiles, BigDecimal chmodDirs, BigDecimal mkdirs,
            BigDecimal setReplication, BigDecimal fileInfo, BigDecimal dirInfo) {
        this.create = create;
        this.read = read;
        this.rename = rename;
        this.delete = delete;
        this.chmodFiles = chmodFiles;
        this.chmodDirs = chmodDirs;
        this.mkdirs = mkdirs;
        this.lsFile = lsFile;
        this.lsDir = lsDir;
        this.setReplication = setReplication;
        this.fileInfo = fileInfo;
        this.dirInfo = dirInfo;
        
        this.rand = new Random(System.currentTimeMillis());
        
        createCoin();
    }

    private void createCoin(){
        
       System.out.println("Percentages create: "+create+" read: "+read+" mkdir: "
               +mkdirs+" rename: "+rename+" delete: "+delete+" lsFile: "
               +lsFile+" lsDir: "+lsDir+" chmod files: "+chmodFiles+" chmod dirs: "+chmodDirs
               +" setReplication: "+setReplication+" fileInfo: "+fileInfo+" dirInfo: "+dirInfo);
       double total = create.doubleValue()+read.doubleValue()+rename.doubleValue()+
               delete.doubleValue()+lsFile.doubleValue()+lsDir.doubleValue()
               +chmodFiles.doubleValue()+chmodDirs.doubleValue()+mkdirs.doubleValue()+setReplication.doubleValue()+fileInfo.doubleValue()+dirInfo.doubleValue();
       if(total != 100){
           throw new IllegalArgumentException("All probabilities should add to 100. Got: "+total);
       }

       for(int i = 0 ; i < create.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.CREATE_FILE);
       }
       
       for(int i = 0 ; i < read.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.READ_FILE);
       }
       
       for(int i = 0 ; i < rename.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.RENAME_FILE);
       }
       
       for(int i = 0 ; i < delete.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.DELETE_FILE);
       }
       
       for(int i = 0 ; i < lsFile.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.LS_FILE);
       }
       
       for(int i = 0 ; i < lsDir.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.LS_DIR);
       }
       
       for(int i = 0 ; i < chmodFiles.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.CHMOD_FILE);
       }
       
       for(int i = 0 ; i < chmodDirs.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.CHMOD_DIR);
       }
       
       for(int i = 0 ; i < mkdirs.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.MKDIRS);
       }
       
       for(int i = 0 ; i < setReplication.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.SET_REPLICATION);
       }
       
       for(int i = 0 ; i < fileInfo.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.FILE_INFO);
       }
       
       for(int i = 0 ; i < dirInfo.doubleValue() * expansion ; i++){
           dice.add(BenchmarkOperations.DIR_INFO);
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
