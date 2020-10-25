package AA;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class NormalActiveUserMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\t");

        if(sArray1.length < 2) return;

        String user = sArray1[0];

        int number = Integer.parseInt(sArray1[1]);

        if(number >= 2 * 180 && number <= 180 * 4){
            context.write(new Text(user), new IntWritable(number));
        }
    }
}
