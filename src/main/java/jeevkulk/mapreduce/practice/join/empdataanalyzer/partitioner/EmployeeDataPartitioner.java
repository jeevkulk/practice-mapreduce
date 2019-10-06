package jeevkulk.mapreduce.practice.join.empdataanalyzer.partitioner;

import jeevkulk.mapreduce.practice.join.empdataanalyzer.domain.Employee;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmployeeDataPartitioner extends Partitioner<IntWritable, Employee> implements Configurable {

    private Configuration configuration;

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    @Override
    public int getPartition(IntWritable mapperKeyMonthOfBirth, Employee employee, int numReduceTasks) {
        return mapperKeyMonthOfBirth.get();
    }
}
