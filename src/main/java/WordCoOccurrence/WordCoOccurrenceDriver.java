package WordCoOccurrence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by quyvc on 06/03/2017.
 */
public class WordCoOccurrenceDriver {
    public static void main(String[]args)
            throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }

        System.out.println("Args = " + Arrays.asList(args));

        Job job = Job.getInstance();
        job.setJarByClass(WordCoOccurrenceDriver.class);
        job.setJobName("Word Co Occurrence");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(StringPairMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(TextPair.class);
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
