package jeevkulk.mapreduce.join.empdataanalyzer;

import jeevkulk.mapreduce.join.empdataanalyzer.domain.Employee;
import jeevkulk.mapreduce.join.empdataanalyzer.mapper.EmployeeDataMapper;
import jeevkulk.mapreduce.join.empdataanalyzer.partitioner.EmployeeDataPartitioner;
import jeevkulk.mapreduce.join.empdataanalyzer.reducer.EmployeeDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import jeevkulk.mapreduce.partitioner.empdataanalyzer.reducer.EmployeeDataReducer.Counter;

import java.io.IOException;

public class EmployeeDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new EmployeeDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {
        getConf().set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(getConf());
        job.setJobName("Employee Data Partitioner");
        job.setJarByClass(EmployeeDriver.class);
        job.setNumReduceTasks(12);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Employee.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(EmployeeDataMapper.class);
        job.setReducerClass(EmployeeDataReducer.class);
        job.setPartitionerClass(EmployeeDataPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            int success = job.waitForCompletion(true) ? 0 : 1;
            if (success == 0) {
                System.out.println("Number of married employees: "+job.getCounters().findCounter(Counter.MARRIED_EMPLOYEES).getValue());
                System.out.println("Number of terminated employees: "+job.getCounters().findCounter(Counter.TERMINATED_EMPLOYEES).getValue());
                System.out.println("Percentage of FEMALE employees: "+(double)job.getCounters().findCounter(Counter.FEMALE_EMPLOYEES).getValue() / (double)job.getCounters().findCounter(Counter.ALL_EMPLOYEES).getValue());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
