package com.company;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.gson.Gson;

public class Main {

    public static long c;
    public static long cTmp = 0;
    public static String startDirectory = "";
    public static boolean useStartDirectory = false;
    public static boolean startDirectoryFound = true;
    public static ConcurrentLinkedQueue queueFiFo = new ConcurrentLinkedQueue();
    public static ConcurrentLinkedQueue queueFiFoDb = new ConcurrentLinkedQueue();
    public static long copyQueueSize;
    public static long databaseQueueSize;

    public static void processDirectory(File dir, String backupBasePath, ArrayList excludePaths, ArrayList excludePatterns) {
        try {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {

                    Boolean doit = true;

                    //Log.info("directory:" + file.getCanonicalPath());
                    Log.info("processing directory: " + file.getCanonicalPath());

                    String currentPath = file.getCanonicalPath();

                    // exclude paths from the exclude paths list
                    for (int i = 0; i < excludePaths.size(); i++) {
                        String exclude = (String) excludePaths.get(i);

                        if( currentPath.startsWith(exclude)){
                            doit = false;
                        }
                    }

                    String tst;

                    if (doit){
                        if(Main.useStartDirectory){

                            if(! startDirectoryFound){
                                tst = currentPath + "/";

                                if(tst.equals(Main.startDirectory)){
                                    startDirectoryFound = true;
                                    Log.important("startDirectoryFound: " + currentPath);
                                    //System.exit(0);
                                }else{
                                    Log.info("Main.startDirectory: " + Main.startDirectory);
                                    Log.info("currentPath: " + currentPath);
                                }
                            }
                        }


                        processDirectory(file, backupBasePath, excludePaths, excludePatterns);
                    } else{
                        Log.info("ignoring excluding directory: " + file.getCanonicalPath());
                    }

                } else {
                    //Log.info("     file:" + file.getCanonicalPath());
                    //Log.info("     file:" + file.getName());

                    Boolean doThisFile = true;

                    String currentPathToFile = file.getCanonicalPath();

                    for (int i = 0; i < excludePatterns.size(); i++) {
                        String exclude = (String) excludePatterns.get(i);


                        if( currentPathToFile.contains(exclude)){
                            doThisFile = false;
                        }
                    }

                    if(Main.useStartDirectory){
                        if(!startDirectoryFound){
                            doThisFile = false;
                        }
                    }


                    if(doThisFile){
                        Log.info("-------------- next file ----------------");
                        long startTs = System.currentTimeMillis();
                        SingleFileProcess.handleFile(file.getCanonicalPath(), file.getName(), backupBasePath);
                        Log.time(startTs, "SingleFileProcess.handleFile");
                    }else{
                        Log.info("Excluding this file: " + currentPathToFile);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
	// write your code here



        File currentDir;

        Log.important("Starting");

        Gson g = new Gson();

        String sys = System.getProperty("os.name");

        Log.important("Operating system is: " + sys);

        //System.exit(0);

        String jsonTxt = com.company.Util.get_txt_from_file("./CondensedBackupConfig" + sys + ".json");

        Config cfg = g.fromJson(jsonTxt, Config.class);

        Log.logInfo = cfg.logInfo;
        Log.logTime = cfg.logTime;
        Log.logQueue = cfg.logQueue;
        Log.logDuplicate = cfg.logDuplicate;

        Database.dbPath = cfg.databasePath;

        DatabaseThread.dbTxtPath = cfg.databaseTxtPath;

        try {
            File yourFile = new File(DatabaseThread.dbTxtPath);
            yourFile.createNewFile();
        }catch(Exception ex){
            Log.important("error creating file: " + cfg.databasePath);
            System.exit(1);
        }



        Log.important("Database path  is: " + cfg.databasePath);

        Database.makeTables();



        //load maps
        Database.loadMaps();

        String backupBasePath = cfg.outputStoragePath;

        Log.important("outputStoragePath is: " + backupBasePath);

        if(cfg.useLatestAsStartPoint){

            Log.important("useLatestAsStartPoint is TRUE ");

            startDirectory = Database.getStartPointDirectory();
            useStartDirectory = true;
            startDirectoryFound = false;
            Log.important("startDirectory = " + startDirectory);
        }


        c = 0;
        String currentPath = "";
        String tst;

        try {
            Thread.sleep(3000);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        // only now we can start the threads

        CopyThread cpt = new CopyThread(queueFiFo);
        DatabaseThread cbT = new DatabaseThread(queueFiFoDb);

        for (int i = 0; i < cfg.includePathList.size(); i++) {
            Log.info((String) cfg.includePathList.get(i));

            currentDir = new File((String) cfg.includePathList.get(i));

            try {
                currentPath = currentDir.getCanonicalPath();
            }catch(Exception ex){
                Log.important("error: " + ex.toString());
                System.exit(0);
            }

            if(Main.useStartDirectory){

                tst = currentPath + "/";
                if(tst.equals(Main.startDirectory)){
                    startDirectoryFound = true;
                    Log.important("startDirectoryFound: " + currentPath);
                    //System.exit(0);
                }else{
                    Log.info("Main.startDirectory: " + Main.startDirectory);
                    Log.info("currentPath: " + currentPath);
                }

            }

            processDirectory(currentDir, backupBasePath, cfg.excludePathList, cfg.excludePatternList);
        }

        Log.important("Done with file loop now waiting for threads to finish.");

        try {
            // if queues too long the make pause
            while(Main.copyQueueSize > 0){
                Log.info("thread needs sleep because copyQueueSize > 0: " + Long.toString(Main.copyQueueSize));
                long start = System.currentTimeMillis();
                Thread.sleep(3000);
                System.out.println("Sleep time in ms = "+(System.currentTimeMillis()-start));
            }

            while(Main.databaseQueueSize > 0){
                Log.info("thread needs sleep because databaseQueueSize > 0: " + Long.toString(Main.databaseQueueSize));
                long start = System.currentTimeMillis();
                Thread.sleep(3000);
                System.out.println("Sleep time in ms = "+(System.currentTimeMillis()-start));
            }
        } catch (Exception e) {
            Log.info("handleFile error in sending thread to sleep  = " + e.toString());
            return;
        }



        Log.important("EVERYTHING DONE. END OF CODE.");
        System.exit(0);


    }
}
