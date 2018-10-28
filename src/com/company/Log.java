package com.company;

public class Log {

    public static boolean logInfo = false;
    public static boolean logTime = false;
    public static boolean logQueue = false;
    public static boolean logDuplicate = false;

    static void info(String s){
        if(logInfo){
            String ts;
            ts = Long.toString(System.currentTimeMillis());
            System.out.println(ts + " : INFO : " + s);
        }
        //
    }


    static void queue(String s){
        if(logQueue){
            String ts;
            ts = Long.toString(System.currentTimeMillis());
            System.out.println(ts + " : QUEUE : " + s);
        }
        //
    }

    static void duplicate(String s){
        if(logDuplicate){
            String ts;
            ts = Long.toString(System.currentTimeMillis());
            System.out.println(ts + " : DUPLICATE : " + s);
        }
        //
    }

    static void time(long tsStart, String s){
        if(logTime){
            long tsEnd;
            tsEnd = System.currentTimeMillis();
            long diff = tsEnd - tsStart;
            String tsDiff = Long.toString(diff);
            String ts;
            ts = Long.toString(System.currentTimeMillis());
            System.out.println(ts + " : TIME : " + s + " milliseconds: " + tsDiff);
        }
        //
    }

    static void important(String s){
        String ts;
        ts = Long.toString(System.currentTimeMillis());
        System.out.println(ts + " : IMPORTANT : " + s);
    }

    static void counter(String s){
        String ts;
        ts = Long.toString(System.currentTimeMillis());
        System.out.println(ts + " : COUNTER : " + s);
    }

    static void error(String s){
        String ts;
        ts = Long.toString(System.currentTimeMillis());
        System.out.println(ts + " : ERROR: " + s);
    }


}
