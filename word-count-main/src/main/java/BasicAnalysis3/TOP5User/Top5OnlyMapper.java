package BasicAnalysis3.TOP5User;

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

        if(user.equals("delicious50")
                ||user.equals("thinkingstiff")
                ||user.equals("dominiquerdr")
                ||user.equals("mariolavandeira")
                ||user.equals("thegamingscoop")
        ){
            StringBuilder sb = new StringBuilder();
            sb
                    .append(user)
                    .append("X=====X")
                    .append(sArray1[2].split("W\t")[1]);

            context.write(new Text(sb.toString()), new Text(""));
        }
    }
}
