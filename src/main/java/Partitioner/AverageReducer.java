package Partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quyvc on 03/03/2017.
 */
public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int lengthWord = 0;
        int frequencyWord = 0;
        for (IntWritable value : values) {
            lengthWord += value.get();
            frequencyWord++;
        }

        if (frequencyWord>0)
            context.write(key, new DoubleWritable(Double.valueOf(lengthWord)/frequencyWord));
    }
}
