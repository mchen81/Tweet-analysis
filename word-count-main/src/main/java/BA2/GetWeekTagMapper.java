package BA2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.WeekCompute;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class GetWeekTagMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //if(!value.toString().startsWith("lol")) return;

        //split a twitter to 4 single lines
        String[] sArray1 = value.toString().split("\\r?\\n");

        if(sArray1.length < 3) return;

        String timeLine =  sArray1[0];

        String[] timeLineArray = timeLine.split(" ");
        if(timeLineArray.length < 2) return;
        String day = timeLineArray[0];
        String[] dayArray = day.split("-");
        if(dayArray.length < 2) return;

        //Step1: extract week and month
        int week = WeekCompute.getWeek(Integer.parseInt(dayArray[1]));
        if(week == 0) return;
        int month = Integer.parseInt(dayArray[0]) - 6;
        week += month * 4;

        //Step2: extract hashTags
        String tweet = sArray1[2];
        if(tweet.endsWith("No Post Title")) return;

        //Step3: start mapping!
        String newKeyPrefix = Integer.toString(week);

        StringTokenizer itr = new StringTokenizer(tweet);
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken()
                    .replaceAll("[^a-zA-Z0-9@#]", "");

            if(token.equals("W")) continue;

            if(!token.startsWith("#")) continue;

            if(token.length() < 2) continue;
            if(token.charAt(1) == '#') continue;

            context.write(new Text(newKeyPrefix + token), new IntWritable(1));
        }
    }
}
