package com.company;


import java.io.BufferedReader;
import java.io.FileReader;

public class Util {


    public static String get_txt_from_file(String path){

        BufferedReader br;
        String everything = "";

        try {

            br = new BufferedReader(new FileReader(path));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            everything = sb.toString();

            br.close();

        } catch(Exception x){

        }

        return everything;

    }


}
