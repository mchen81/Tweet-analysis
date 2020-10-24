package BA1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class UserSortReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(
            IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        // calculate the total count
        for(Text val : values){
            sb.append(val).append("\t");
        }
        context.write(key, new Text(sb.toString()));
    }

}
