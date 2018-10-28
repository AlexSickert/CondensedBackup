package com.company;

import java.io.File;
import java.math.BigInteger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentLinkedQueue;


public class SingleFileProcess {




    public static void handleFile(String totalPath, String FileName, String backupBasePath){

        long maxSize = 1000;

        try {
            // if queues too long the make pause
            if(Main.copyQueueSize > maxSize){
                Log.info("thread needs sleep because copyQueueSize > maxSize" );
                long start = System.currentTimeMillis();
                Thread.sleep(1000);
                System.out.println("Sleep time in ms = "+(System.currentTimeMillis()-start));
            }

            if(Main.databaseQueueSize > maxSize){
                Log.info("thread needs sleep because databaseQueueSize > maxSize" );
                long start = System.currentTimeMillis();
                Thread.sleep(1000);
                System.out.println("Sleep time in ms = "+(System.currentTimeMillis()-start));
            }
        } catch (Exception e) {
            Log.info("handleFile error in sending thread to sleep  = " + e.toString());
            return;
        }

        Main.c = Main.c + 1;

        if(Main.cTmp > 99){
            Log.counter("file_counter: " + Long.toString(Main.c));
            Log.counter("copyQueueSize: " + Long.toString(Main.copyQueueSize));
            Log.counter("databaseQueueSize: " + Long.toString(Main.databaseQueueSize));
            Main.cTmp = 0;
        }

        Main.cTmp += 1;

        long startTs = System.currentTimeMillis();
        String sum = CheckSum.getCheckSum(totalPath);
        Log.time(startTs, "CheckSum.getCheckSum");

        long i = 0;

        try {
            File file = new File(totalPath);
            Log.info("File size: " + Long.toString(file.length()));
            i = file.length();
        } catch (Exception e) {
            Log.info("error getting file size of  = " + totalPath);
            return;
        }

        SingleFileProcess.addFile(totalPath, FileName, i, sum, backupBasePath);

    }

    private static void addFile(String totalPath, String FileName, long size, String checkSum, String backupBasePath){

        Log.info("processing file: " + FileName);

        // check if already in database
        long i = Database.getIdOfCheckSumMap(checkSum, size);

        long startTs = System.currentTimeMillis();
        // if smaller than 0 then so far not in database
        if(i < 0){

            Log.info("Adding checkSum and copy file  = " + checkSum);
            i = Database.insertCheckSum(checkSum, size);
            Log.info("File name will be = " + Long.toString(i) + ".dat");
            // if it is not in the database then we need to copy the file to the backup
            String path = copyFile(totalPath, Long.toString(i) + ".dat", backupBasePath);
            // now register the path in database
            Database.updateCheckSum(path, i);

        }else{
            Log.duplicate("duplicate - checkSum already exists  = " + checkSum + " filename: " + FileName);
            Log.info("checkSum already exists  = " + checkSum);
        }
        Log.time(startTs, "1 - addFile");


        // get id of the file name
        // and if not, insert it
        long j = Database.getIdOfFileNameMap(FileName);
        startTs = System.currentTimeMillis();
        // if smaller than 0 then not in databse yet
        if(j < 0){
            Log.info("Adding FileName  = " + FileName);
            j = Database.insertFileName(FileName);
            Log.info("FileName id  = " + Long.toString(j));
        }else{
            Log.duplicate("FileName already exists  = " + FileName);
        }
        Log.time(startTs, "2 - addFile");


        // check if the file path exists in the table and if not then insert it
        // the toalPath includes the file name so we need to get rid of it

        startTs = System.currentTimeMillis();

        String path = totalPath.substring(0, totalPath.length() - FileName.length());

        long k = Database.getIdOfFilePathMap(path);

        if(k < 0){
            Log.info("Adding FilePath = " + path);
            Database.insertFilePath(path);
            k = Database.getIdOfFilePathMap(path);
        }else{
            Log.duplicate("FilePath already exists  = " + path);
        }

        Log.time(startTs, "3 - addFile");
        startTs = System.currentTimeMillis();

        // now we need to associate the path id with the file id
        // the path id is "k" and the file id is "j"

        long m = Database.getIdOfFilePathNameDictionaryMap(k, j, i);

        if(m < 0){
            Log.info("FilePath/Name mapping does not exist in db  = " + Long.toString(k) + "/" + Long.toString(j));
            m = Database.insertFilePathNameDictionary(k,j, i);
            Log.info("FilePath/Name mapping id is no  = " + Long.toString(m));
        }else{
            Log.info("FilePath to file connection already exists  = " + Long.toString(m));
        }

        Log.time(startTs, "4 - addFile");
    }


    private static String copyFile(String totalPath, String fileName, String backupBasePath){

        String folderOuter = Integer.toString(ThreadLocalRandom.current().nextInt(1, 201));
        String folderInner = Integer.toString(ThreadLocalRandom.current().nextInt(1, 201));

        String fullBackupPath = backupBasePath + "/" + folderOuter + "/" + folderInner;
        new File(fullBackupPath).mkdirs();

        String totalPathOut = fullBackupPath + "/" + fileName;

        try {
            // Todo: Make thread and queue and handle it there

            //copyFileUsingFileStreams(totalPathFile, totalPathOutFile);
            addToQueue(totalPath, totalPathOut);

        }catch(Exception e){
            Log.info("Error " + e.toString());
        }

        return fullBackupPath;
    }


    private static void addToQueue(String totalPathFile, String totalPathOutFile){

        QueueEntry q = new QueueEntry();
        q.fromFolder = totalPathFile;
        q.toFolder = totalPathOutFile;

        Main.queueFiFo.add(q);

    }

}
