package CreateSequenceFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by quyvc on 06/03/2017.
 */
public class ReadSequenceFile {
    public static void main(String[]args)
            throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }

        System.out.println("Args = " + Arrays.asList(args));

        Job job = Job.getInstance();
        job.setJarByClass(ReadSequenceFile.class);
        job.setJobName("Avg Word Length");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);

    }

}
