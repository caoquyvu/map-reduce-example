package CustomWritableComparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by quyvc on 06/03/2017.
 */
public class StringPairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private final static IntWritable length = new IntWritable(1);
    private TextPair wordObject = new TextPair();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[]words = line.split("\\W+");
        wordObject.set(new Text(words[0]),new Text(words[1]));
        context.write(wordObject,length);

    }
}
