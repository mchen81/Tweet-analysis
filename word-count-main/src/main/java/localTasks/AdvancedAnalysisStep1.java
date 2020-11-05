package localTasks;

import java.io.*;
import java.util.ArrayList;

public class AdvancedAnalysisStep1 {
    /**
     * Modified these before run this code
     *
     */
    static String inputPath = "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/AA/step1_candidates.txt";
    static String outputPath = "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/AA/step2_shortlist";

    public static void main(String[] args) {
        chooseEvery1000(inputPath, outputPath);
    }


    private static void chooseEvery1000(String inputPath, String outputPath){

        try{
            //step1: load all candidates
            BufferedReader br = new BufferedReader(new FileReader(inputPath));

            //step2: every 1000 lines, choose 1 lucky guy
            ArrayList<String> luckyGuys = new ArrayList<>();

            String line = br.readLine();
            int counter = 0;

            while(line != null){
                counter++;
                if(counter < 1000){
                    line = br.readLine();
                    continue;
                }

                counter = 0;

                String[] sA = line.split("\t");
                if(sA.length < 2){
                    line = br.readLine();
                    continue;
                }

                luckyGuys.add(sA[0]);

                line  = br.readLine();
            }

            //step3: write lucky guys to target file
            File outFile = new File(outputPath);
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

            for(String name : luckyGuys) {
                bw.write(name);
                bw.newLine();
            }

            bw.flush();
            bw.close();
        } catch (Exception e){
            System.out.println(e);
        }
    }
}
