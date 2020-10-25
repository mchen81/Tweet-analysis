package localTasks;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

public class BA2 {
    public static void main(String[] args) {
//        new BA2().findTop5("/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step3_top5Tag"
//                , "/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step2_sortedTags"
//                );

        new BA2().trend("/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step4_trend"
                        , "/Users/jcliu/Projects/usfGit/cs677/Project2/BA/BA2/step2_sortedTags"
                , "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/BA2/step3_top5Tag"
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

                 //   System.out.println(hotTag);
//                    System.out.println(hotTag);
                }

                line  = br.readLine();
            }

            //step2: count trend
            br = new BufferedReader(new FileReader(metDataPath));

            line = br.readLine();
            int i = 0;
            while(line != null && i++ < 5000){
                String[] sA1 = line.split("\\t");
                if(sA1.length < 2) continue;

                String weekAndTag = sA1[1];

                int week = Integer.parseInt(weekAndTag
                        .split("#")[0]
                );

                String tag = weekAndTag.split("#")[1];

               // System.out.println(tag);

                if(!hotTagSet.contains(tag)){
                    line = br.readLine();
                    continue;
                }

                System.out.println(tag);

                int[] trend = map.getOrDefault(tag, new int[24]);

                trend[week - 1] = Integer.parseInt(sA1[0]);

                map.put(tag, trend);

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

                int week = Integer.parseInt(sA1[1]
                    .split("#")[0]
                );

                PriorityQueue<String> queue = map.getOrDefault(
                        week,
                        new PriorityQueue<>(new TagComparator())
                );

                queue.add(line);

                map.put(week, queue);

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
