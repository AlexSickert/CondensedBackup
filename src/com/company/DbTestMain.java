package com.company;

import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DbTestMain {

    public static long c;
    public static long cTmp = 0;
    public static String startDirectory = "";
    public static boolean useStartDirectory = false;
    public static boolean startDirectoryFound = true;
    public static ConcurrentLinkedQueue queueFiFo = new ConcurrentLinkedQueue();
    public static ConcurrentLinkedQueue queueFiFoDb = new ConcurrentLinkedQueue();


    public static void main(String[] args) {
	// write your code here


        File currentDir;

        Log.important("Starting");

        Gson g = new Gson();

        String sys = System.getProperty("os.name");

        Log.important("Operating system is: " + sys);

        //System.exit(0);

        String jsonTxt = Util.get_txt_from_file("./CondensedBackupConfig" + sys + ".json");

        Config cfg = g.fromJson(jsonTxt, Config.class);

        Log.logInfo = cfg.logInfo;
        Log.logTime = cfg.logTime;
        Log.logQueue = cfg.logQueue;

        Database.dbPath = cfg.databasePath;

        Log.important("Database path  is: " + cfg.databasePath);

        Database.dumpContent("check_sums", " id = 18 limit  10");
        Database.dumpContent("file_names", " id in (20, 21) limit  10");
        Database.dumpContent("file_names_paths_map", "1=1 limit  20");
        //Database.dumpContent("file_paths", "id in (1015, 1016, 1017, 1018, 1019) limit 5");
        //Database.dumpContent("file_paths", "file_name_id = 1748 limit 5");
        Database.dumpContent("file_paths", " id = 4  limit  10");





    }
}
