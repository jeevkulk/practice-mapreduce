package jeevkulk.mapreduce.join.reduceside;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new MRJoinDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJobName("Mapreduce jeevkulk.mapreduce.join");
        job.setJarByClass(MRJoinDriver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MRJoinReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SecondMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

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
