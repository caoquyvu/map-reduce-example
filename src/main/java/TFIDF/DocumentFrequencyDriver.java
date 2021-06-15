package TFIDF;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import javax.swing.text.Document;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by quyvc on 07/03/2017.
 */
public class DocumentFrequencyDriver  {
    public static void main(String[]args)
            throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }
        System.out.println("Args "+ Arrays.asList(args));
        Job job = Job.getInstance();
        job.setJarByClass(DocumentFrequencyDriver.class);
        job.setJobName("Compute Document Frequency");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DocumentFrequencyMapper.class);
        job.setReducerClass(DocumentFrequencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}