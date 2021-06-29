package WordLength; /**
 * Created by quyvc on 02/03/2017.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text,
        IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text wordObject = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                int length = word.length();
                String wordMapping = "lớn";
                if (length==1) wordMapping = "rất nhỏ";
                else if (length<=4) wordMapping="nhỏ";
                else if (length<=9) wordMapping="trung bình";
                wordObject.set(wordMapping);
                context.write(wordObject, one);
            }
        }
    }
}
