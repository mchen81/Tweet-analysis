package BA1.whatAndWhen.what;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WhatMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 3) return;

//        String[] sArray2 = sArray1[2].split("W\t");
//
//        if(sArray2.length < 1) return;
//
//        String twitter = sArray2[0];

        //ignore/skip No Post Title
        if(sArray1[2].endsWith("No Post Title")) return;

        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(sArray1[2].toString());
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken().replaceAll("[^a-zA-Z0-9@#]", "");

            if(token.equals("W")) continue;

            context.write(new Text(token), new IntWritable(1));
        }
    }
}
