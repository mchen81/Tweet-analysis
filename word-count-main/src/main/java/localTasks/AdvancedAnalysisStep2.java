package localTasks;

import org.checkerframework.checker.units.qual.A;
import util.SentimentAnalysis;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * This step, for every hour(0-23), we choose 100 tweets(same reason, I only have $100 free credit) from AA/step3_lucky120Tweets
 * And use Azure Cognitive Services API to do sentiment analyze
 * To use it, you need to hardcode output and input path below
 */
public class AdvancedAnalysisStep2 {
    //path of lucky120Tweets
    static final String inputPath = "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/AA/step3_lucky120Tweets";

    static final String outputPath = "";

    public static void main(String[] args) {
        //step1: prepare Map<hour, tweets>
        HashMap<Integer, ArrayList<String>> map = fileProcess(inputPath);

        //step2: for each hour, choose 100 tweets
        HashMap<Integer, ArrayList<String>> mapLimited100 = fileProcess(inputPath);
        for(int i = 0; i < 24; i++){
            ArrayList<String> list100 =  new ArrayList<>();
            ArrayList<String> list =  map.get(i);

            if(list.size() <= 100){
                mapLimited100.put(i, list);
                continue;
            }

            int jump = list.size() / 100;
            int j = 0;

            while(list100.size() < 100){
                list100.add(list.get(j));
                j += jump;
            }

            mapLimited100.put(i, list100);
        }


        //step2: call api and print score
        for(int i = 0; i < 24; i++){
            System.out.println("Hour: " + i + "  Score: " + SentimentAnalysis.sentimentAnalysis(mapLimited100.get(i), args[0]));
          //  System.out.println(mapLimited100.get(i).size());
        }

//        for(int i = 0; i < 24; i++){
//            System.out.println("Hour: " + i + "  Score: " + mapLimited100.get(i).size());
//
//            for(String s : mapLimited100.get(i)) System.out.println(s);
//        }
    }

    private static HashMap<Integer, ArrayList<String>> fileProcess(String path){
        HashMap<Integer, ArrayList<String>> map = new HashMap<>();

        try{
            //step1: load all candidates
            BufferedReader br = new BufferedReader(new FileReader(inputPath));

            String line = br.readLine();

            while(line != null){
                String[] sArray = line.split("\t")[0].split("X===X");

                if(sArray.length < 2){
                    line  = br.readLine();
                    continue;
                }

                if(sArray[1] == null || sArray[1].contains("RT") || sArray[1].isEmpty()){
                    line = br.readLine();
                    continue;
                }

                int hour = Integer.parseInt(sArray[0]);

                ArrayList<String> list = map.getOrDefault(hour, new ArrayList<>());

                list.add(sArray[1]);
                map.put(hour, list);

                line  = br.readLine();
            }
        } catch (Exception e){
            System.out.println(e);
        }

        return map;
    }
}
