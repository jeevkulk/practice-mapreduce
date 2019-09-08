package jeevkulk.mapreduce.partitioner.empdataanalyzer.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import jeevkulk.mapreduce.partitioner.empdataanalyzer.domain.Employee;

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
