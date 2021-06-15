package WordCoOccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quyvc on 06/03/2017.
 */
public class SortedOccurrenceReducer extends Reducer<IntWritable, TextPair, TextPair, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<TextPair> values, Context context)
            throws IOException, InterruptedException {

        for (TextPair value : values) {
            context.write(value,key);
        }
    }
}