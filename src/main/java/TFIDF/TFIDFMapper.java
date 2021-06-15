package TFIDF;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by quyvc on 07/03/2017.
 */
public class TFIDFMapper extends Mapper<Text, Text, Text ,
        DoubleWritable> {
    private final static DoubleWritable tfidf = new DoubleWritable(1.0);

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        int tf = Integer.valueOf(value.toString().split(",")[0]);
        int df = Integer.valueOf(value.toString().split(",")[1]);

        int numOfDocument = Integer.valueOf(System.getProperty("numOfDocument"));
        tfidf.set(1.0*tf*df/numOfDocument);
        context.write(key,tfidf);
    }
}