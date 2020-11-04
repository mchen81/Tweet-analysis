package FinalProject;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utilities;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class JsonToTextMapper
extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        JsonObject jsonObject = null;

        try{
            jsonObject = new JsonParser().parse(value.toString()).getAsJsonObject();
        } catch (Exception ignored){

        }


        //sanity checks
        if(jsonObject == null || jsonObject.isJsonNull() || (!jsonObject.isJsonObject())) return;
        if((!jsonObject.has("reviewerID"))
                || (!jsonObject.has("asin")) || (!jsonObject.has("reviewTime"))) return;

        StringBuilder sb = new StringBuilder();

        sb.append(jsonObject.get("asin")).append("\t");
        sb.append(jsonObject.get("reviewerID")).append("\t");
        sb.append(jsonObject.get("reviewTime"));

        NullWritable nullWritable = NullWritable.get();
        context.write(new Text(sb.toString()), nullWritable);
    }
}
