package AdvancedAnalysis.Step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * find all tweets that belong to lucky120(see Constants/constants)
 * and transform their format to $HourX===X$Tweet
 */
public class GetLucky120Tweets extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //delimiter
        conf.set("textinputformat.record.delimiter", "T\t2009-");

        /* Job Name. You'll see this in the YARN webapp */
        Job job = Job.getInstance(conf, "AA2");

        /* Current class */
        job.setJarByClass(GetLucky120Tweets.class);

        /* Mapper class */
        job.setMapperClass(GetLucky120TweetsMapper.class);

        /* Combiner class. Combiners are run between the Map and Reduce
         * phases to reduce the amount of output that must be transmitted.
         * In some situations, we can actually use the Reducer as a Combiner
         * but ONLY if its inputs and ouputs match up correctly. The
         * combiner is disabled here, but the following can be uncommented
         * for this particular job:
        //job.setCombinerClass(WordCountReducer.class);

        /* Reducer class */
        job.setReducerClass(GetLucky120TweetsReducer.class);

        /* Outputs from the Mapper. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /* Outputs from the Reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /* Reduce tasks */
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Wait (block) for the job to complete... */
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new GetLucky120Tweets(), args);
        System.exit(exitCode);
    }
}
