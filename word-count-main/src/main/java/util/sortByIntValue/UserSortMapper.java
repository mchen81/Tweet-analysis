package util.sortByIntValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class UserSortMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\t");

        if(sArray1.length != 2) return;

//        StringTokenizer st = new StringTokenizer(value.toString());
//
//        Text text;
//        if(st.hasMoreTokens()) text = new Text(st.nextToken());
//        else return;
//        IntWritable intWritable;
//        if(st.hasMoreTokens()) intWritable = new IntWritable(Integer.parseInt(st.nextToken()));
//        else return;

        context.write(new IntWritable(Integer.parseInt(sArray1[1]))
                , new Text(sArray1[0]));
    }
}
