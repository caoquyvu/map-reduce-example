package WordCoOccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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
        String firstWord=words[0];
        String secondWord;
        for(int i=1;i<words.length;i++){
            secondWord=words[i];
            wordObject.set(new Text(firstWord),new Text(secondWord));
            firstWord=secondWord;
            context.write(wordObject,length);
        }

    }
}
