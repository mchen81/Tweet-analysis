package BA1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 2) return;

        //extract username
        //String[] sArray2 = sArray1[0].split("/");

       // if(sArray2.length < 2) return;
//
    //    if(!sArray2[sArray2.length - 2].equals("twitter.com")) return;

      //  String user = sArray2[sArray2.length - 1];

        String user = sArray1[0];

        //ignore/skip No Post Title
        if(sArray1[1].endsWith("No Post Title")) return;

        //emit/output result
        context.write(new Text(user), new IntWritable(1));

//        // tokenize into words.
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        // emit word, count pairs.
//        while (itr.hasMoreTokens()) {
//            String token = itr.nextToken().replaceAll("[^a-zA-Z0-9@#]", "");
//
//            if(token.equals("W")) continue;
//
//            context.write(new Text(token), new IntWritable(1));
//        }
    }
}
