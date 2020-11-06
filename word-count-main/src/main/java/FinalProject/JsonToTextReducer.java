package FinalProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JsonToTextReducer
extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<NullWritable> values, Context context)
    throws IOException, InterruptedException {
        NullWritable nullWritable = NullWritable.get();
        context.write(key, nullWritable);
    }
}
