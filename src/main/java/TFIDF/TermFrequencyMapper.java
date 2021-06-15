package TFIDF;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quyvc on 07/03/2017.
 */
public class TermFrequencyMapper extends Mapper<LongWritable, Text, Text ,
        IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text wordObject = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        String fileName = path.getName();

        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                wordObject.set(word+","+fileName);
                context.write(wordObject, one);
            }
        }
    }
}