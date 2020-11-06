package AdvancedAnalysis.AnalyzeByDictionary.ComputeScore;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer:
 * Output: Total sentiment scores in KV<Time, SentimentScore> format
 */
public class AASentimentAnalysisReducer
extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<LongWritable> values, Context context)
    throws IOException, InterruptedException {
        long score = 0;
        // calculate the total count
        for(LongWritable val : values){
            score += val.get();
        }
        context.write(key, new LongWritable(score));
    }
}
