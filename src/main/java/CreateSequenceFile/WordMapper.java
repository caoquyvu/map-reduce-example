package CreateSequenceFile; /**
 * Created by quyvc on 02/03/2017.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<Text, IntWritable, Text,
        IntWritable> {
    private Text wordObject = new Text();

    @Override
    public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {

        wordObject.set(key);
        context.write(wordObject, value);


    }
}
