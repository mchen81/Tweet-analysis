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

public class HashtagSentimentAnalysis {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            throw new IllegalArgumentException("[HashtagSentimentAnalysis] Usage: input output");
        }

        Configuration configuration = new Configuration();


        Job job = Job.getInstance(configuration, "HashtagSentimentAnalysis");
        job.setJarByClass(HashtagSentimentAnalysis.class);

        // map and reduce class set up
        job.setMapperClass(HashtagSentimentAnalysisMapper.class);
        job.setReducerClass(HashtagSentimentAnalysisReducer.class);
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


    public static class HashtagSentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public static Set<String> hashTagSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            new WordSentimentUtil();
            hashTagSet.add("#140mafia");
            hashTagSet.add("#fb");
            hashTagSet.add("#ff");
            hashTagSet.add("#iranelection");
            hashTagSet.add("#jobs");
            hashTagSet.add("#tcot");
            hashTagSet.add("#mobsterworld");
            hashTagSet.add("#imthankfulfor");
            hashTagSet.add("#iaintafraidtosay");
            hashTagSet.add("#wheniwaslittle");
            hashTagSet.add("#justbecause");
            hashTagSet.add("#donttrytoholla");
            hashTagSet.add("#in2020");
            hashTagSet.add("#09memories");
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
            String theHahTag = null;
            int score = 0;
            while (stringTokenizer.hasMoreTokens()) {
                String word = stringTokenizer.nextToken().toLowerCase();
                if (hashTagSet.contains(word)) {
                    theHahTag = word;
                }
                score += WordSentimentUtil.getWordScore(word);
            }
            if (theHahTag != null) {
                if (score > 0) {
                    score = 1;
                } else if (score < 0) {
                    score = -1;
                }
                context.write(new Text(theHahTag), new IntWritable(score));
            }
        }
    }

    public static class HashtagSentimentAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
