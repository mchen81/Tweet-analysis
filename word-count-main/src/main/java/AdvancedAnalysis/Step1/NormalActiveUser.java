package AdvancedAnalysis.Step1;

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
 * The first step of Advanced Analysis.
 * In Step1, we regard users who post an average of 2-4 tweets each day as "Normal Active Users"
 * We use NormalActiveUser to find out the list of "Normal Active Users".
 *
 * Next step: Use AnalyzeByDictionary to do sentiment analysis
 */
public class NormalActiveUser extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        /* Job Name. You'll see this in the YARN webapp */
        Job job = Job.getInstance(conf, "word count job");

        /* Current class */
        job.setJarByClass(NormalActiveUser.class);

        /* Mapper class */
        job.setMapperClass(NormalActiveUserMapper.class);

       // job.setSortComparatorClass(UserTweetCountComparator.class);

        /* Combiner class. Combiners are run between the Map and Reduce
         * phases to reduce the amount of output that must be transmitted.
         * In some situations, we can actually use the Reducer as a Combiner
         * but ONLY if its inputs and ouputs match up correctly. The
         * combiner is disabled here, but the following can be uncommented
         * for this particular job:
        //job.setCombinerClass(WordCountReducer.class);

        /* Reducer class */
        job.setReducerClass(NormalActiveUserReducer.class);

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
                new NormalActiveUser(), args);
        System.exit(exitCode);
    }
}
