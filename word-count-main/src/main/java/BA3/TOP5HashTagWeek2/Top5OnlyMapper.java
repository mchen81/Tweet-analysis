package BA3.TOP5HashTagWeek2;

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
public class Top5OnlyMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 3) return;

        String[] sArray2 = sArray1[1].split("/");

        if(sArray2.length < 3) return;
        String user = sArray2[sArray2.length - 1];

        if(sArray1[2].endsWith("No Post Title")) return;
        if(sArray1[2].contains("RT")) return;

        String tweet = sArray1[2].split("W\t")[1];

        if(tweet.contains("#iranelection")){
            StringBuilder sb = new StringBuilder();
            sb
                    .append("iranelection")
                    .append("X=====X")
                    .append(tweet);

            context.write(new Text(sb.toString()), new Text(""));
        }

        if(tweet.contains("#jobs")){
            StringBuilder sb = new StringBuilder();
            sb
                    .append("jobs")
                    .append("X=====X")
                    .append(tweet);

            context.write(new Text(sb.toString()), new Text(""));
        }

        if(tweet.contains("#followfriday")){
            StringBuilder sb = new StringBuilder();
            sb
                    .append("followfriday")
                    .append("X=====X")
                    .append(tweet);

            context.write(new Text(sb.toString()), new Text(""));
        }

        if(tweet.contains("#spymaster")){
            StringBuilder sb = new StringBuilder();
            sb
                    .append("spymaster")
                    .append("X=====X")
                    .append(tweet);

            context.write(new Text(sb.toString()), new Text(""));
        }

        if(tweet.contains("#iremember")){
            StringBuilder sb = new StringBuilder();
            sb
                    .append("iremember")
                    .append("X=====X")
                    .append(tweet);

            context.write(new Text(sb.toString()), new Text(""));
        }
    }
}
