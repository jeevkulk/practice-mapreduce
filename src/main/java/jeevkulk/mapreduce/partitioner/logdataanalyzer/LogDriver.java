package jeevkulk.mapreduce.partitioner.logdataanalyzer;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class LogDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new LogDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {

        Job job = Job.getInstance(getConf());
        job.setJobName("Log Partitioner");
        job.setJarByClass(LogDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setPartitionerClass(LogPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
