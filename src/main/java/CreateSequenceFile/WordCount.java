package CreateSequenceFile; /**
 * Created by quyvc on 02/03/2017.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class WordCount {
    public static void main(String[]args)
        throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }
        System.out.println("args"+ Arrays.asList(args));
        Job job = Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
