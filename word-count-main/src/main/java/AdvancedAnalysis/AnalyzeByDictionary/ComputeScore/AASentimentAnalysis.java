package AdvancedAnalysis.AnalyzeByDictionary.ComputeScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.NormalActiveUserCheck;
import util.WordSentimentUtil;

/**
 * Compute the total mood scores of each hour(0 - 23)
 *
 * input: All Tweet data(from June to December)
 */
public class AASentimentAnalysis extends Configured implements Tool {

    static {
        new NormalActiveUserCheck();
        new WordSentimentUtil();
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //delimiter
        conf.set("textinputformat.record.delimiter", "T\t2009-");

        /* Job Name. You'll see this in the YARN webapp */
        Job job = Job.getInstance(conf, "AA2");

        /* Current class */
        job.setJarByClass(AASentimentAnalysis.class);

        /* Mapper class */
        job.setMapperClass(AASentimentAnalysisMapper.class);

        /* Combiner class. Combiners are run between the Map and Reduce
         * phases to reduce the amount of output that must be transmitted.
         * In some situations, we can actually use the Reducer as a Combiner
         * but ONLY if its inputs and ouputs match up correctly. The
         * combiner is disabled here, but the following can be uncommented
         * for this particular job:
        //job.setCombinerClass(WordCountReducer.class);

        /* Reducer class */
        job.setReducerClass(AASentimentAnalysisReducer.class);

        /* Outputs from the Mapper. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /* Outputs from the Reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

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
                new AASentimentAnalysis(), args);
        System.exit(exitCode);
    }
}
