package TFIDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quyvc on 07/03/2017.
 */
public class DocumentFrequencyMapper extends Mapper<Text, IntWritable, Text ,
        Text> {
    private Text keyObject = new Text();
    private Text valueObject = new Text();

    @Override
    public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {

        keyObject.set(key.toString().split(",")[0]);
        valueObject.set(key.toString().split(",")[1]+","+value.toString()+","+1);
        context.write(keyObject,valueObject);

    }
}