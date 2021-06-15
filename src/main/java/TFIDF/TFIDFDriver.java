package TFIDF;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by quyvc on 07/03/2017.
 */
public class TFIDFDriver {
    public static void main(String[]args)
            throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }
        System.out.println("Args "+ Arrays.asList(args));
        Job job = Job.getInstance();
        job.setJarByClass(TFIDFDriver.class);
        job.setJobName("Compute TF-IDF");

        FileInputFormat.setInputPaths(job, new Path("data"));
        Configuration conf = job.getConfiguration();
        String dirs = conf.get("mapred.input.dir");
        String[] arrDirs = dirs.split(",");
        int numDirs = arrDirs.length;
        System.setProperty("numOfDocument",String.valueOf(numDirs));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TFIDFMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
