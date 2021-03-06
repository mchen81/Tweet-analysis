package AdvancedAnalysis.Step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Output: KV<normalActiveUserName, TweetNumber>
 */
public class NormalActiveUserReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }

}
