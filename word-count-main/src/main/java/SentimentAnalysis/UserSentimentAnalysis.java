package SentimentAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.Utilities;
import util.WordSentimentUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class UserSentimentAnalysis {

    public static Set<String> userSet = new HashSet<>();

    static {
        new WordSentimentUtil();
        userSet.add("delicious50");
        userSet.add("thinkingstiff");
        userSet.add("dominiquerdr");
        userSet.add("mariolavandeira");
        userSet.add("thegamingscoop");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException("[UserSentimentAnalysis] Must give -input and -output path");
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "UserSentimentAnalysis");
        job.setJarByClass(UserSentimentAnalysis.class);

        // map and reduce class set up
        job.setMapperClass(UserSentimentAnalysis.UserSentimentAnalysisMapper.class);
        job.setReducerClass(UserSentimentAnalysis.UserSentimentAnalysisReducer.class);
        job.setCombinerClass(UserSentimentAnalysis.UserSentimentAnalysisReducer.class);
        // map output set up
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reduce output set up
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // input/output set up
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class UserSentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * Input format should be: user date time contents
         *
         * @param key     doen not matter
         * @param value   input text by line
         * @param context mapper context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            String user = Utilities.getTweetUser(stringTokenizer.nextToken());
            if (userSet.contains(user)) {
                stringTokenizer.nextToken(); // date
                stringTokenizer.nextToken(); // time
                int score = 0;
                while (stringTokenizer.hasMoreTokens()) {
                    score += WordSentimentUtil.getWordScore(stringTokenizer.nextToken());
                }
                context.write(new Text(user), new IntWritable(score));
            }
        }
    }

    public static class UserSentimentAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int score = 0;
            for (IntWritable i : values) {
                score += i.get();
            }
            context.write(key, new IntWritable(score));
        }
    }

}
