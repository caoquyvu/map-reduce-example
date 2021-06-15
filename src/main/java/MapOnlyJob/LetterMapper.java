package MapOnlyJob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quyvc on 03/03/2017.
 */
public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable length = new IntWritable(1);
    private Text wordObject = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                String firstCharacter = word.substring(0,1);
                wordObject.set(firstCharacter);
                length.set(word.length());
                context.write(wordObject, length);
            }
        }
    }
}
