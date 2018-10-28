package com.company;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DatabaseThread implements Runnable {


    protected ConcurrentLinkedQueue queue = null;

    Thread t;

    public static String dbTxtPath = "";

    public DatabaseThread(ConcurrentLinkedQueue queue) {
        this.queue = queue;
        t = new Thread(this, "DatabaseThread");
        System.out.println("New DatabaseThread: " + t);
        Log.important("New DatabaseThread: " + t);
        t.start();
    }


    public void run() {

        DbEntry q = null;
        long qs;
        String sql;
        PreparedStatement pstmt;
        Connection conn = Database.connect();


        try {

            while (true) {


                if(queue.size() > 0) {
                    if(conn.isClosed()){
                        conn = Database.connect();
                    }

                }

                while (queue.size() > 0) {

                    qs = queue.size();

                    Main.databaseQueueSize = qs;

                    try {

                        Log.queue("DatabaseThread queue size: " + Long.toString(qs));
                        q = (DbEntry) queue.poll();
                        long startTs = System.currentTimeMillis();

                        //copyFileUsingFileStreams(q.fromFolder, q.toFolder);



                        switch (q.callName) {

                            case "insertCheckSum":
                                sql = "INSERT INTO check_sums (checksum,size, id) VALUES(?,?,?)";
                                pstmt = conn.prepareStatement(sql);
                                pstmt.setString(1, q.param1);
                                //System.out.println("insertCheckSum: " + q.param2);
                                //System.out.println(Long.parseLong(q.param2));
                                pstmt.setLong(2, Long.parseLong(q.param2));
                                pstmt.setLong(3, Long.parseLong(q.param3));
                                pstmt.executeUpdate();
                                break;

                            case "updateCheckSum":
                                sql = "UPDATE check_sums set backup_path = ? WHERE  id = ?";
                                //System.out.println(q.param1);
                                pstmt = conn.prepareStatement(sql);
                                pstmt.setString(1, q.param1);
                                pstmt.setLong(2, Long.parseLong(q.param2));
                                pstmt.executeUpdate();
                                break;

                            case "insertFilePathNameDictionary":
                                sql = "INSERT INTO file_names_paths_map (file_checksum_id, file_name_id,file_path_id, id) VALUES(?,?, ?, ?)";
                                pstmt = conn.prepareStatement(sql);
                                pstmt.setLong(1, Long.parseLong(q.param1));
                                pstmt.setLong(2, Long.parseLong(q.param2));
                                pstmt.setLong(3, Long.parseLong(q.param3));
                                pstmt.setLong(4, Long.parseLong(q.param4));
                                pstmt.executeUpdate();
                                break;

                            case "insertFileName":
                                sql = "INSERT INTO file_names (file_name, id) VALUES(?,?)";
                                pstmt = conn.prepareStatement(sql);
                                //pstmt.setLong(1, Long.parseLong(q.param1));
                                pstmt.setString(1, q.param2);
                                pstmt.setLong(2, Long.parseLong(q.param3));
                                pstmt.executeUpdate();
                                break;

                            case "insertFilePath":
                                sql = "INSERT INTO file_paths (file_path, id) VALUES(?, ?)";
                                pstmt = conn.prepareStatement(sql);
                                pstmt.setString(1, q.param1);
                                pstmt.setLong(2, Long.parseLong(q.param2));
                                pstmt.executeUpdate();
                                break;
                        }

                        String contentToAppend = "";

                        contentToAppend += q.callName;
                        contentToAppend += "|";
                        contentToAppend += q.param1;
                        contentToAppend += "|";
                        contentToAppend += q.param2;
                        contentToAppend += "|";
                        contentToAppend += q.param3;
                        contentToAppend += "|";
                        contentToAppend += q.param4;
                        contentToAppend += "|\n";

                        appendToFile(contentToAppend);

                        Log.time(startTs, "DatabaseThread insert");

                    } catch (Exception e) {

                        if(q != null){
                            Log.error("DatabaseThread: " + q.callName);
                            Log.error("DatabaseThread: " + q.param1);
                            Log.error("DatabaseThread: " + q.param1);
                            Log.error("DatabaseThread: " + q.param1);
                            Log.error("DatabaseThread: " + q.param1);
                        }

                        e.printStackTrace();
                        System.exit(0);
                    }

                }

                if(! conn.isClosed()){

                    conn.commit();
                    conn.close();
                }

                Log.queue("DatabaseThread sleeping 1000");
                Thread.sleep(1000);

            }

        } catch (Exception e) {
            Log.error("DatabaseThread: " + e.toString());
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void appendToFile(String contentToAppend){

        try {
            Files.write(Paths.get(dbTxtPath), contentToAppend.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Problems occured when appending to file : " + dbTxtPath);
            e.printStackTrace();
        }
    }
}
