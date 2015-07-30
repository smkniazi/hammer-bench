///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package se.sics.hops.experiments.workload.generator;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Random;
//import java.util.UUID;
///**
// *
// * @author salman
// * 
// */
//public class NaiveFileGenerator implements FilePool {
//
//    private Random rand;
//    private UUID uuid = null;
//    protected List<String> allThreadFiles;
//    private String baseDir;
//    protected String threadDir;
//    private String machineName = "";
//    private int depth;
//
//    public NaiveFileGenerator(String baseDir, int depth) {
//        this.allThreadFiles = new LinkedList<String>();
//        this.rand = new Random(System.currentTimeMillis());
//        this.baseDir = baseDir;
//        uuid = UUID.randomUUID();
//        try {
//            machineName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            machineName = "Client_Machine+" + rand.nextInt();
//        }
//
//        if (!baseDir.endsWith("/")) {
//            threadDir = baseDir + "/";
//        }
//        threadDir = threadDir + "_" + machineName + "_" + uuid;
//
//        String[] comp = PathUtils.getPathNames(threadDir);
//
//        if (comp.length < depth) {
//            for (int i = comp.length; i < (depth); i++) {
//                threadDir += "/depth_" + i;
//            }
//        }
//    }
//
//    public NaiveFileGenerator(String baseDir) {
//        this(baseDir, 0);
//    }
//
//    private String getThreadDir() {
//        return threadDir;
//    }
//
//    @Override
//    public String getFileToCreate() {
//        ThreadLocal<Integer> index = new ThreadLocal<Integer>();
//        Integer i = index.get();
//        if (i == null) {
//            i = new Integer(0);
//            index.set(new Integer(0));
//        }
//        i = i++;
//        index.set(i);
//
//        String path = threadDir + "/file_" + i;
//        allThreadFiles.add(path);
//        //System.out.println("Create Path "+path);
//        return path;
//    }
//
//    @Override
//    public void fileCreationFailed(String file) {
//        allThreadFiles.remove(file);
//    }
//
//    @Override
//    public String getFileToRead() {
//        if (allThreadFiles.isEmpty()) {
//            return null;
//        }
//        int readIndex = rand.nextInt(allThreadFiles.size());
//        String path = allThreadFiles.get(readIndex);
//        //System.out.println("Read Path "+path);
//        return path;
//    }
//
//    @Override
//    public String getFileToRename() {
//        if (allThreadFiles.isEmpty()) {
//            return null;
//        }
//        int renameIndex = rand.nextInt(allThreadFiles.size());
//        String path = allThreadFiles.get(renameIndex);
//        //System.out.println("Rename path "+path);
//        return path;
//    }
//
//    @Override
//    public void fileRenamed(String from, String to) {
//        int index = allThreadFiles.indexOf(from);
//        allThreadFiles.remove(index);
//        allThreadFiles.add(index, to);
//    }
//
//    @Override
//    public String getFileToDelete() {
//        if (allThreadFiles.isEmpty()) {
//            return null;
//        }
//        int deleteIndex = rand.nextInt(allThreadFiles.size());
//        String file = allThreadFiles.remove(deleteIndex);
//        //System.out.println("Delete Path "+file);
//        return file;
//    }
//
//    @Override
//    public String getDirToList() {
//        if (allThreadFiles.isEmpty()) {
//            return null;
//        }
//        int index = rand.nextInt(allThreadFiles.size());
//        String path = allThreadFiles.get(index);
//        //path is pointing to a file
//        //randomly retrun file and dir paths
//        //if(rand.nextInt(2) == 1){ // dir
//        int dirIndex = path.lastIndexOf("/");
//        path = path.substring(0, dirIndex);
//        //}
//
//        //System.out.println("List Path "+path);
//        return path;
//    }
//
//    @Override
//    public String getPathToChangePermissions() {
//        if (allThreadFiles.isEmpty()) {
//            return null;
//        }
//        int index = rand.nextInt(allThreadFiles.size());
//        String path = allThreadFiles.get(index);
//        //System.out.println("Chmod Path "+path);
//        return path;
//    }
//}
