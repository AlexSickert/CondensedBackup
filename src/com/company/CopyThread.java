package com.company;
import java.io.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.nio.channels.FileChannel;

public class CopyThread implements Runnable{

    protected ConcurrentLinkedQueue queue = null;

    Thread t;

    public CopyThread(ConcurrentLinkedQueue queue) {
        this.queue = queue;
        t = new Thread(this, "CopyThread");
        System.out.println("New thread: " + t);
        Log.important("New thread: " + t);
        t.start();
    }


    public void run() {

        QueueEntry q;
        long qs;

        try {

            while(true){

                qs = queue.size();

                Main.copyQueueSize = qs;

                if(queue.size() > 0){
                    Log.queue("CopyThread queue size: " + Long.toString(qs));
                    q = (QueueEntry) queue.poll();
                    long startTs = System.currentTimeMillis();
                    //copyFileUsingFileStreams(q.fromFolder, q.toFolder);
                    copyFileUsingChannel(q.fromFolder, q.toFolder);
                    Log.time(startTs, "CopyThread Copy File");

                }else{
                    Log.queue("CopyThread sleeping 1000");
                    Thread.sleep(1000);
                }
            }

        } catch (Exception e) {
            Log.error("CopyThred: " + e.toString());
            e.printStackTrace();
        }
    }


    private static void copyFileUsingFileStreams(String source, String dest)

            throws IOException {

        File totalPathFile = new File(source);
        File totalPathOutFile = new File(dest);

        Log.queue(source);
        Log.queue(dest);

        InputStream input = null;
        OutputStream output = null;

        try {
            input = new FileInputStream(totalPathFile);
            output = new FileOutputStream(totalPathOutFile);
            byte[] buf = new byte[1024];
            int bytesRead;

            while ((bytesRead = input.read(buf)) > 0) {
                output.write(buf, 0, bytesRead);
            }

        } finally {
            input.close();
            output.close();
        }
    }



    private static void copyFileUsingChannel(String source, String dest) throws IOException {
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        try {

            File sourceFile = new File(source);
            File destFile = new File(dest);


            sourceChannel = new FileInputStream(sourceFile).getChannel();
            destChannel = new FileOutputStream(destFile).getChannel();
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        }finally{
            sourceChannel.close();
            destChannel.close();
        }
    }


}
