package Partitioner;


import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by quyvc on 06/03/2017.
 */
public class MyPartition<Text,IntWritable> extends Partitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text key,IntWritable value, int numReduceTasks){
        System.out.println(key.toString().toLowerCase().charAt(0)-97);

        return (key.toString().toLowerCase().charAt(0)-97)%numReduceTasks;
    }
}
