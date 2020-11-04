package BasicAnalysis3;

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
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class WordCountJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //delimiter
        conf.set("textinputformat.record.delimiter", "U\thttp://twitter.com/");

        /* Job Name. You'll see this in the YARN webapp */
        Job job = Job.getInstance(conf, "word count job");

        /* Current class */
        job.setJarByClass(WordCountJob.class);

        /* Mapper class */
        job.setMapperClass(WordCountMapper.class);

       // job.setSortComparatorClass(UserTweetCountComparator.class);

        /* Combiner class. Combiners are run between the Map and Reduce
         * phases to reduce the amount of output that must be transmitted.
         * In some situations, we can actually use the Reducer as a Combiner
         * but ONLY if its inputs and ouputs match up correctly. The
         * combiner is disabled here, but the following can be uncommented
         * for this particular job:
        //job.setCombinerClass(WordCountReducer.class);

        /* Reducer class */
        job.setReducerClass(WordCountReducer.class);

        /* Outputs from the Mapper. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /* Outputs from the Reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /* Reduce tasks */
        job.setNumReduceTasks(1);

//        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4);
//
//        job.setOutputFormatClass(FileOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//      //  LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//       // FileInputFormat.addInputPath(job, new Path(args[0]));
//        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
      //  FileOutputFormat.setOutputPath(job, new Path(args[1]));

//            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Wait (block) for the job to complete... */
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new WordCountJob(), args);
        System.exit(exitCode);
    }
}
