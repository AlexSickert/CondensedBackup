package com.company;

import java.sql.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class Database {

    static String dbPath = "";

    static HashMap CheckSums = new HashMap();
    static HashMap FileNamesPath = new HashMap();
    static HashMap FileNames = new HashMap();
    static HashMap FilePaths = new HashMap();

    static long idCheckSums = 1;
    static long idFileNamesPath = 1;
    static long idFileNames = 1;
    static long idFilePaths = 1;
    static Connection conn = null;

    private static String getId(long id, String str){
        String s1 = Long.toString(id);
        String ret;
        ret = s1 + "-" + str;
        return ret;
    }

    private static String getId(String idStr, long id2){
        String s1 = idStr;
        String s2 = Long.toString(id2);
        String ret;
        ret = s1 + "-" + s2;
        return ret;
    }

    private static String getId(long id, long id2, long id3){
        String s1 = Long.toString(id);
        String s2 = Long.toString(id2);
        String s3 = Long.toString(id3);
        String ret;
        ret = s1 + "-" + s2 + "-" + s3;
        return ret;
    }

    private static String getId(long id, long id2){
        String s1 = Long.toString(id);
        String s2 = Long.toString(id2);
        String ret;
        ret = s1 + "-" + s2;
        return ret;
    }

    public static Connection connect() {

        if(conn == null){
            try {
                // db parameters
                String url = "jdbc:sqlite:" + dbPath;
                // create a connection to the database
                conn = DriverManager.getConnection(url);
                conn.setAutoCommit(false);


                //System.out.println("Connection to SQLite has been established.");

            } catch (SQLException e) {
                System.out.println("Connection connect(): " + e.getMessage());
            }
        }else{
            try {
                if(conn.isClosed()){
                    // db parameters
                    String url = "jdbc:sqlite:" + dbPath;
                    // create a connection to the database
                    conn = DriverManager.getConnection(url);
                    conn.setAutoCommit(false);
                }

                //System.out.println("Connection to SQLite has been established.");

            } catch (SQLException e) {
                System.out.println("Connection connect(): " + e.getMessage());
            }
        }

        return conn;

    }

    public static long insertCheckSum(String checkSum, long size){

        //String sql = "INSERT INTO check_sums (checksum,size, id) VALUES(?,?,?)";
        long id = -1;

        try{
//            Connection conn = Database.connect();
//            PreparedStatement pstmt = conn.prepareStatement(sql);
//            pstmt.setString(1, checkSum);
//            pstmt.setLong(2, size);
            idCheckSums += 1;
//            pstmt.setLong(3, idCheckSums);
//            pstmt.executeUpdate();

            //System.out.println("insertCheckSum: " + Long.toString(size));

            DbEntry EbE = new DbEntry();
            EbE.callName = "insertCheckSum";
            EbE.param1 = checkSum;
            EbE.param2 = Long.toString(size);
            EbE.param3 = Long.toString(idCheckSums);
            Main.queueFiFoDb.add(EbE);


            // if we are here it is safe to insert into map
            //id = Database.getIdOfCheckSumSql(checkSum, size);
            String idKey = getId(size, checkSum);
            CheckSums.put(idKey, idCheckSums);
            return idCheckSums;

        } catch (Exception e) {
            System.out.println("insertCheckSum(String checkSum, long size)" + e.getMessage());
        }

        return id;


    }


    public static long getIdOfCheckSumMap(String checkSum, long size){

        //change this into a lookup in list
        String idKey = getId(size, checkSum);
        long ret = -1;
        Object retObj;

        retObj = CheckSums.get(idKey);

        if(retObj == null){
            ret = -1;
        }else {
            ret = (long) retObj;
        }

        return ret;
    }

    // check_sums    backup_path

    public static void updateCheckSum(String backupPath, long id){

        //String sql = "UPDATE check_sums set backup_path = ? WHERE  id = ?";

        try{

            DbEntry EbE = new DbEntry();
            EbE.callName = "updateCheckSum";
            EbE.param1 = backupPath;
            EbE.param2 = Long.toString(id);
            Main.queueFiFoDb.add(EbE);

        } catch (Exception e) {
            System.out.println("updateCheckSum(String backupPath, long id): " + e.getMessage());
        }

    }

    public static long getIdOfFilePathNameDictionaryMap(long file_id, long path_id, long check_sum_id){

        String idKey = getId(file_id, path_id, check_sum_id);

        //int i = -1;
        Object retObj;

        retObj = FileNamesPath.get(idKey);

        if(retObj ==  null){
            return -1;
        }else{
            return (long) retObj;
        }
    }


    public static long insertFilePathNameDictionary(long path_id, long file_id , long check_sum_id){

        long i = -1;

        try{
            idFileNamesPath += 1;
            DbEntry EbE = new DbEntry();
            EbE.callName = "insertFilePathNameDictionary";
            EbE.param2 = Long.toString(file_id);
            EbE.param3 = Long.toString(path_id);
            EbE.param4 = Long.toString(idFileNamesPath);
            EbE.param1 = Long.toString(check_sum_id);
            Main.queueFiFoDb.add(EbE);
            String idStr = getId(file_id, path_id, check_sum_id);
            FileNamesPath.put(idStr, idFileNamesPath);
            return idFileNamesPath;
        } catch (Exception e) {
            System.out.println("insertFilePathNameDictionary(Integer file_id, Integer path_id): " + e.getMessage());
        }

        return i;
    }


    public static long getIdOfFileNameMap(String file_name){

        String idStr = file_name;

        Object retObj;
        retObj = FileNames.get(idStr);

        if(retObj == null){
            return -1;
        }else{
            return (long) retObj;
        }
    }


    public static long insertFileName(String file_name){

        //String sql = "INSERT INTO file_names (id_checksum,file_name, id) VALUES(?,?,?)";

        long ret = -1;
        //String idStr = getId(file_name, id);
        String idStr = file_name;

        try{
//            Connection conn = Database.connect();
//            PreparedStatement pstmt = conn.prepareStatement(sql);
//            pstmt.setLong(1, id);
//            pstmt.setString(2, file_name);
            idFileNames += 1;
//            pstmt.setLong(3, idFileNames);
//            pstmt.executeUpdate();


            DbEntry EbE = new DbEntry();
            EbE.callName = "insertFileName";
            EbE.param1 = "";  // not used anymore
            EbE.param2 = file_name;
            EbE.param3 = Long.toString(idFileNames);
            Main.queueFiFoDb.add(EbE);



            //ret = Database.getIdOfFileNameSql(file_name, id);
            FileNames.put(idStr, idFileNames);
            return idFileNames;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return ret;
    }



    public static String getStartPointDirectory(){



        String sql = "SELECT max(id) as id FROM file_paths ";

        int i = -1;

        try {
            Connection conn = Database.connect();
            PreparedStatement pstmt  = conn.prepareStatement(sql);
            ResultSet rs  = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                i = rs.getInt("id");
            }
            conn.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


        sql = "SELECT file_path FROM file_paths where id = " + Integer.toString(i);
        String ret = "";
        try {
            Connection conn = Database.connect();
            PreparedStatement pstmt  = conn.prepareStatement(sql);
            // set the value
            //pstmt.setLong(1,fileNameId);
            //pstmt.setString(1,path_name);
            //
            ResultSet rs  = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                ret = rs.getString("file_path");
            }
            conn.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return ret;

    }

    public static long getIdOfFilePathMap(String path_name){

        //String sql = "SELECT id FROM file_paths WHERE file_path = ?";

        int i = -1;
        Object retObj;

        retObj = FilePaths.get(path_name);

        if(retObj == null){
            return -1;
        }else{
            return (long) retObj;
        }
    }


    public static int insertFilePath(String file_path){

        //String sql = "INSERT INTO file_paths (file_path, id) VALUES(?, ?)";

        int i = -1;

        try {

//            Connection conn = Database.connect();
//            PreparedStatement pstmt = conn.prepareStatement(sql);
//            pstmt.setString(1, file_path);
            idFilePaths += 1;
//            pstmt.setLong(2, idFilePaths);
//            pstmt.executeUpdate();
            //i = Database.getIdOfFilePathSql(file_path);
            FilePaths.put(file_path, idFilePaths);

            DbEntry EbE = new DbEntry();
            EbE.callName = "insertFilePath";
            EbE.param1 = file_path;
            EbE.param2 = Long.toString(idFilePaths);
            Main.queueFiFoDb.add(EbE);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return i;
    }

    /**
     * Create the necessary tables in the database
     */
    public static void makeTables() {

        String sql = "CREATE TABLE IF NOT EXISTS check_sums ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "size INTEGER,"
                + "backup_path text,"
                + "checksum text )";

        Database.executeStatement(sql);

        sql = "CREATE TABLE IF NOT EXISTS file_names ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "id_checksum INTEGER,"
                + "file_name text )";

        Database.executeStatement(sql);

        sql = "CREATE TABLE IF NOT EXISTS file_names_paths_map ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "file_checksum_id INTEGER,"
                + "file_name_id INTEGER,"
                + "file_path_id INTEGER )";

        Database.executeStatement(sql);

        sql = "CREATE TABLE IF NOT EXISTS file_paths ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "file_path text )";

        Database.executeStatement(sql);


    }

    static ResultSet getRs(String sql){

        ResultSet rs;

        Log.info("getRs(String sql)");

        try {

            Connection conn = Database.connect();
            PreparedStatement pstmt  = conn.prepareStatement(sql);

            Log.info("getRs(String sql)  -  executing");
            rs  = pstmt.executeQuery();
            Log.info("getRs(String sql)  -  executed");
            if(rs == null){
                Log.info("getRs(String sql)  -  RS IS NULL");
            }else{
                Log.info("getRs(String sql)  -  RS IS NOT NULL");
            }
            return rs;

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    static void loadMaps(){

        Log.important("Loading maps");

        ResultSet rs;


        long id;
        long id_compare = 0;
        int checkSum;
        int size;
        String idKey;
        int fileNameId;
        int filePathId;
        int idCheckSum;
        String fileName;
        String filePath;
        int fileCheckSumId;

        rs = getRs("SELECT checksum,size, id FROM check_sums order by id asc");

        try {
            id_compare = 0;
            while (rs.next()) {
                id = rs.getLong("id");
                checkSum = rs.getInt("checksum");
                size = rs.getInt("size");
                idKey = getId(size, checkSum);
                Log.info("Adding to CheckSums");
                CheckSums.put(idKey, id);

                if(id > id_compare){
                    idCheckSums = id;
                    id_compare = id;
                }
            }

        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        rs = getRs("SELECT id_checksum,file_name, id FROM file_names order by id asc");

        try {
            id_compare = 0;
            while (rs.next()) {
                id = rs.getLong("id");
                idCheckSum = rs.getInt("id_checksum");
                fileName = rs.getString("file_name");
                //idKey = getId(fileName, idCheckSum);
                idKey = fileName;
                Log.info("Adding to FileNames");
                FileNames.put(idKey, id);

                if(id > id_compare){
                    idFileNames = id;
                    id_compare = id;
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        rs = getRs("SELECT file_name_id,file_path_id, file_checksum_id, id FROM file_names_paths_map order by id asc");

        try {
            id_compare = 0;
            while (rs.next()) {
                id = rs.getLong("id");
                fileNameId = rs.getInt("file_name_id");
                filePathId = rs.getInt("file_path_id");
                fileCheckSumId = rs.getInt("file_checksum_id");

                idKey = getId(fileNameId, filePathId, fileCheckSumId);
                Log.info("Adding to FileNamesPath");
                FileNamesPath.put(idKey, id);

                if(id > id_compare){
                    idFileNamesPath = id;
                    id_compare = id;
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }


        rs = getRs("SELECT file_path, id FROM file_paths order by id asc");

        try {
            id_compare = 0;
            while (rs.next()) {
                id = rs.getLong("id");
                filePath = rs.getString("file_path");
                Log.info("Adding to FilePaths");
                FilePaths.put(filePath, id);

                if(id > id_compare){
                    idFilePaths = id;
                    id_compare = id;
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        Log.important("Done loading maps");
    }


    /**
     *
     * @param sql
     */
    private static void executeStatement(String sql) {


        try  {

            Connection conn = Database.connect();
            PreparedStatement pstmt = conn.prepareStatement(sql);

            pstmt.executeUpdate();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }


    /**
     * for testing dump all content of a given table
     * @param tableName
     */
    public static void dumpContent(String tableName, String where){

        System.out.println("=============================================================");

        String sql = "SELECT * FROM " + tableName + " where " + where;
        System.out.println(sql);

        try (Connection conn = Database.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();


            // loop through the result set
            ArrayList<String> columns = new ArrayList<String>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metadata.getColumnName(i);
                columns.add(columnName);
            }

            while (rs.next()) {
                System.out.println("----------------------------------------");
                for (String columnName : columns) {
                    String value = rs.getString(columnName);
                    System.out.println(tableName + ": " + columnName + " = " + value);
//                    System.out.println(value + "\t");
                }
            }


        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    public static void dumpGroupedFiles(){

        String sql = "SELECT file_name, count(file_path) FROM file_paths group by file_name";

        try (Connection conn = Database.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();


            // loop through the result set
            ArrayList<String> columns = new ArrayList<String>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metadata.getColumnName(i);
                columns.add(columnName);
            }

            String currentFileName = "";

            while (rs.next()) {
                for (String columnName : columns) {
                    String value = rs.getString(columnName);

                    //System.out.println("-----------------------------" );

                    //System.out.println("file_name" + ": " + columnName + " = " + value);

                    if(columnName.trim().equals("file_name")){
                        currentFileName = value;
                    }

                    if(columnName.trim().equals("count(file_path)")){
                        //System.out.println("file_name" + ": " + columnName + " = " + value);
                        if(value.trim().equals("3") ){
                            System.out.println("file_name" + ": " + columnName + " = " + value);

                            String sql2 = "select * from file_names where id = " + currentFileName;

                            dumpRecordSet(sql2);
                        }
                    }
//                    System.out.println(value + "\t");
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    public static void dumpRecordSet(String sql){

        try (Connection conn = Database.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();

            // loop through the result set
            ArrayList<String> columns = new ArrayList<String>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metadata.getColumnName(i);
                columns.add(columnName);
            }
            while (rs.next()) {
                for (String columnName : columns) {
                    String value = rs.getString(columnName);
                    System.out.println( columnName + " = " + value);
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
