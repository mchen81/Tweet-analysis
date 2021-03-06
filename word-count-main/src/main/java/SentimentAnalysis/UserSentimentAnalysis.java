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
import util.WordSentimentUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class UserSentimentAnalysis {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            throw new IllegalArgumentException("[UserSentimentAnalysis] Usage: input output");
        }

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "UserSentimentAnalysis");
        job.setJarByClass(UserSentimentAnalysis.class);

        // map and reduce class set up
        job.setMapperClass(UserSentimentAnalysisMapper.class);
        job.setReducerClass(UserSentimentAnalysisReducer.class);
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
        public static Set<String> userSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            new WordSentimentUtil();

            userSet.add("delicious50");
            userSet.add("thinkingstiff");
            userSet.add("dominiquerdr");
            userSet.add("mariolavandeira");
            userSet.add("thegamingscoop");
        }

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
            if (stringTokenizer.countTokens() < 3) {
                return;
            }
            String user = stringTokenizer.nextToken();
            if (userSet.contains(user)) {
                stringTokenizer.nextToken(); // date
                stringTokenizer.nextToken(); // time
                int score = 0;
                while (stringTokenizer.hasMoreTokens()) {
                    score += WordSentimentUtil.getWordScore(stringTokenizer.nextToken());
                }
                if (score > 0) {
                    score = 1;
                } else if (score < 0) {
                    score = -1;
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
            int finalScore = 0;

            context.write(key, new IntWritable(score));
        }
    }

}
