package BasicAnalysis1.whatAndWhen.when;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 *
 * Output format:
 * X=====X
 * time
 * username
 * twitter
 */
public class WhenMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 3 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 3) return;

        //step1: get time
        String[] sArray2 = sArray1[0].split(" ");
        if(sArray2.length < 2) return;

        String[] sArray3 = sArray2[sArray2.length - 1].split(":");
        if(sArray3.length < 3) return;

        int time = Integer.parseInt(sArray3[0]);

        //22-06, 07-13, 14-21

        if(time >= 22 || time <= 6){
            context.write(new Text(sArray1[1] + ": 22 ~ 06"), new IntWritable(1));
        }
        else if(time >= 7 && time <= 13){
            context.write(new Text(sArray1[1] + ": 07 ~ 13"), new IntWritable(1));
        }
        else {
            context.write(new Text(sArray1[1] + ": 14 ~ 21"), new IntWritable(1));
        }

        return;
    }
}
