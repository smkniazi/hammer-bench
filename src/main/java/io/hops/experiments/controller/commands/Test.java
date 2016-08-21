//package io.hops.experiments.controller.commands;
//
///**
// * Created by salman on 8/11/16.
// */
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
//public class Test {
//
//  public static void main(String argv[]) {
//
//    new Test().start();
//
//
//  }
//
//  //Number of bits for Block size
//  final short BLOCK_BITS = 48;
//  final short HAS_BLOCK_BITS = 8;
//  final short REPLICATION_BITS = 8;
//
//  //Header mask 64-bit representation
//  //Format:[8 bits for has blocks][8 bits for replication][48 bits for PreferredBlockSize]
//  final long MASK_HAS_BLKS =    0xFF00000000000000L;
//  final long MASK_REPLICATION = 0x00FF000000000000L;
//  final long MASK_BLOCK_SIZE =  0x0000FFFFFFFFFFFFL;
//  long header;
//
//  public void start() {
////    setPreferredBlockSizeNoPersistance(16);
//    setReplicationNoPersistance((short)16);
//
//    short val = extractBlockReplication(header);
////    long val2 = extractBlockSize(header);
//
//
//  }
//
//   void setReplicationNoPersistance(short replication) {
//    if (replication <= 0 || replication > 255) {
//      throw new IllegalArgumentException("Unexpected value for the " +
//              "replication [" + replication + "]. Expected [1:255]");
//    }
//
//    header = ((long) replication << BLOCK_BITS) | (header & ~MASK_REPLICATION);
//  }
//
//  short extractBlockReplication(long header) {
//    long val =  (header & MASK_REPLICATION) ;
//    long val2 = val >> BLOCK_BITS;
//    return (short) val2;
//  }
//
//  long extractBlockSize(long header) {
//    return header & MASK_BLOCK_SIZE;
//  }
//
//  void setPreferredBlockSizeNoPersistance(long preferredBlkSize) {
//    if ((preferredBlkSize < 0) || (preferredBlkSize > ~HEADERMASK)) {
//      throw new IllegalArgumentException("Unexpected value for the block " +
//              "size [" + preferredBlkSize + "]");
//    }
//    header = (header & HEADERMASK) | (preferredBlkSize & ~HEADERMASK);
//  }
//  public void hash(String argv[]) {
//
//    Map<Integer, String> map = new HashMap<Integer, String>();
//
//    try {
//      File file = new File("/tmp/client-names");
//      FileReader fileReader = new FileReader(file);
//      BufferedReader bufferedReader = new BufferedReader(fileReader);
//      String line;
//      while ((line = bufferedReader.readLine()) != null) {
//        int hash = line.hashCode();
//        if (map.containsKey(hash)) {
//          System.out.println("Hash collision ");
//          System.out.println(hash + " -> " + line);
//          System.out.println(hash + " -> " + map.get(hash));
//        } else {
//          map.put(hash, line);
//        }
//      }
//      System.out.println("Unique Hashes " + map.size());
//      fileReader.close();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//
//  }
//}
