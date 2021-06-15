package TFIDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by quyvc on 07/03/2017.
 */
public class DocumentFrequencyReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int numberOfDocumentContainTerm = 0;
        for (Text value : values) {
            numberOfDocumentContainTerm++;
        }
        for (Text value : values) {
            Text keyReducer = new Text(key.toString() + ","
                    + value.toString().split(",")[0]);
            Text valueReducer = new Text(value.toString().split(",")[1] + ","
                    + String.valueOf(numberOfDocumentContainTerm));
            context.write(keyReducer, valueReducer);
        }
    }
}
