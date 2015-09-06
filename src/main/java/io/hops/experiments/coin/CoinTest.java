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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.utils.BenchmarkUtils;
import java.math.BigDecimal;

/**
 *
 * @author salman
 */
public class CoinTest {
    public static void main(String [] argv) throws FileNotFoundException, IOException{
      
      
        MultiFaceCoin coin = new MultiFaceCoin(
                new BigDecimal(5),
                new BigDecimal(5),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(10),
                new BigDecimal(5),
                new BigDecimal(5),
                new BigDecimal(5),
                new BigDecimal(5),
                new BigDecimal(0),
                new BigDecimal(0)
                );
        HashMap<BenchmarkOperations,Integer> map  = new HashMap<BenchmarkOperations,Integer>();
        
        int times = 100000;
        
        for(int i =0 ; i < times;i++){
            BenchmarkOperations op = coin.flip();
            
            Integer count = map.get(op);
            if(count == null){
                count = new Integer(0);
                map.put(op,count);
            }
            count = count + 1;
            map.put(op, count);
        }
        
        
        for(BenchmarkOperations op:map.keySet()){
            System.out.println("Operation "+op+" Count "+map.get(op)+" = "+BenchmarkUtils.round(((map.get(op)/(double)times)*100))+"%");
        }
        

    }
}
