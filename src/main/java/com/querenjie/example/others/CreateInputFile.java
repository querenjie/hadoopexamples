package com.querenjie.example.others;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateInputFile {

    public static String filePath = "D:\\test\\hadoop data\\wordcount data\\in\\inputFile1.txt";
    public static String[] words = {"hi", "earth", "planet", "solar", "energy", "english", "hadoop", "evict", "balance", "guess", "me", "you", "less", "more", "ass", "benefit"};

    public static void writeFile() {
        String lineContent = "";
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
            for (long i = 0L; i < 10000000; i++) {
                lineContent = genLineContent();
                bw.write(lineContent);
                bw.flush();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String genLineContent() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            sb.append(words[(int)(Math.random()*100 % words.length)]).append(" ");
        }
        sb.append("\n");
        String lineContent = sb.toString();
        System.out.println(lineContent);
        return lineContent;
    }

    public static void main(String[] args) {
        writeFile();
    }
}
