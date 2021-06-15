package WordCoOccurrence;

/**
 * Created by quyvc on 06/03/2017.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class WordCoOccurrenceDriverSortedDescending {
    public static void main(String[]args)
            throws IOException,InterruptedException,ClassNotFoundException{
        if (args.length != 2) {
            System.out.printf("Usage:  <input dir> <output dir>\n");
            System.exit(-1);
        }

        System.out.println("Args = " + Arrays.asList(args));

        Job job = Job.getInstance();
        job.setJarByClass(WordCoOccurrenceDriverSortedDescending.class);
        job.setJobName("Word Co Occurrence Sorted Descending");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(InverseMapper.class);
        job.setReducerClass(SortedOccurrenceReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TextPair.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

