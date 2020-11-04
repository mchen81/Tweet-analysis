package localTasks;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

public class BasicAnalysis2 {
    public static void main(String[] args) {
        new BasicAnalysis2().findTop5("/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step3_top5TagV3"
                , "/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step2_sortedTagsV2"
                );
        new BasicAnalysis2().trend("/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step4_trendV3"
                        , "/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step2_sortedTagsV2"
                , "/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step3_top5TagV3"
        );

    }


    private void trend(String outputPath, String metDataPath, String hotTagPath){
        HashMap<String, int[]> map = new HashMap<>();

        HashSet<String> hotTagSet = new HashSet<>();

        BufferedReader br;

        try{

            //step1: load hotTags
            br = new BufferedReader(new FileReader(hotTagPath));

            String line = br.readLine();
            while(line != null){
                String[] sA = line.split("#");
                if(sA.length >= 2){
                    String hotTag = sA[1];

                    hotTag = hotTag.replaceAll("[^a-zA-Z0-9@#]", "");

                    hotTagSet.add(hotTag);
                }

                line  = br.readLine();
            }

            //step2: count trend
            br = new BufferedReader(new FileReader(metDataPath));

            line = br.readLine();
            int i = 0;
            while(line != null){
                String[] sA1 = line.split("\\t");
                if(sA1.length < 2) {
                    line = br.readLine();
                    continue;
                }
                for(int j = 1; j < sA1.length; j++){
                    String ss = sA1[j];

                    String weekAndTag = ss;

                    String tag = weekAndTag.split("#")[1];

                    int week = Integer.parseInt(weekAndTag
                            .split("#")[0]
                    );

                    if(!hotTagSet.contains(tag)){
                        continue;
                    }

                    int[] trend = map.getOrDefault(tag, new int[29]);

                    trend[week - 1] += Integer.parseInt(sA1[0]);

                    map.put(tag, trend);
                }

                line = br.readLine();
            }

            //step3: write trend to new file
            File outFile = new File(outputPath);
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

            for(String key : map.keySet()){
                bw.write("tag " + key);
                bw.newLine();

                int[] trend = map.get(key);
                for(int j = 0; j < trend.length; j++){
                    bw.write(Integer.toString(j + 1) + ": " + trend[j]);
                    bw.newLine();
                }

                bw.flush();
            }

            bw.flush();
            bw.close();
        } catch (Exception e){
            System.out.println(e);
        }
    }

    private void findTop5(String outputPath, String inputPath){
        BufferedReader br;

        HashMap<Integer, PriorityQueue<String>> map = new HashMap<>();

        try{
            br = new BufferedReader(new FileReader(inputPath));

            String line = br.readLine();
            int i = 0;
            while(line != null && i++ < 1000){
                System.out.println(line);
                String[] sA1 = line.split("\\t");
                if(sA1.length < 2) continue;

                for(int j = 1; j < sA1.length; j++){
                    String ss = sA1[j];

                    int week = Integer.parseInt(ss
                            .split("#")[0]
                    );



                    PriorityQueue<String> queue = map.getOrDefault(
                            week,
                            new PriorityQueue<>(new TagComparator())
                    );

                    queue.add(sA1[0] + "\t" + ss);

                    map.put(week, queue);
                }

                line = br.readLine();
            }

            //write to new file
            File outFile = new File(outputPath);
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

            for(int key : map.keySet()){
                bw.write("week " + key);
                bw.newLine();

                int j = 0;
                for(String s : map.get(key)){
                    bw.write(s);
                    bw.newLine();

                    if(j++ == 5) break;
                }

                bw.flush();
            }

            bw.flush();
            bw.close();
        } catch (Exception e){
            System.out.println(e);
        }
    }

    /**
     * receive string format: "22159\t19#beatcancer"
     */
    static class TagComparator implements Comparator<String>{
        @Override
        public int compare(String o1, String o2) {
            return Integer.parseInt(o2.split("\t")[0])
                    - Integer.parseInt(o1.split("\t")[0]);
        }
    }
}
