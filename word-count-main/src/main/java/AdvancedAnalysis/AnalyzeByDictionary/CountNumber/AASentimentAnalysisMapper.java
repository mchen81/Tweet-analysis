package AdvancedAnalysis.AnalyzeByDictionary.CountNumber;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.NormalActiveUserCheck;
import util.Utilities;
import util.WordSentimentUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 */
public class AASentimentAnalysisMapper
extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 3) return;

        int hour = gerHour(sArray1[0]);
        String user = getUser(sArray1[1]);
        String tweet = getTweet(sArray1[2]);

        //sanity check
        if(hour == -1 || user == null || tweet == null) return;
        if(!NormalActiveUserCheck.isNormalActiveUser(user)) return;

        context.write(new Text(Integer.toString(hour)), new LongWritable(1));
    }

    private int gerHour(String line){
        String[] array1 = line.split(" ");
        if(array1.length < 2) return -1;

        String[] array2 = array1[array1.length - 1].split(":");
        if(array2.length < 3) return -1;

        return Integer.parseInt(array2[array2.length - 3]);
    }

    private String getUser(String line){
        String[] array1 = line.split("/");
        if (array1.length < 1) return null;

        return array1[array1.length - 1];
    }

    private String getTweet(String line){
        String[] array1 = line.split("\t");
        if(array1.length < 2) return null;

        String tweet = array1[1];

        if(tweet.endsWith("No Post Title")) return null;

        return tweet;
    }
}
