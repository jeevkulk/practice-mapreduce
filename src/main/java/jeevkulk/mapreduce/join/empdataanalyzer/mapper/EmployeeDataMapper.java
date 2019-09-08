package jeevkulk.mapreduce.join.empdataanalyzer.mapper;

import jeevkulk.mapreduce.join.empdataanalyzer.domain.Employee;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EmployeeDataMapper extends Mapper<LongWritable, Text, IntWritable, Employee> {

    private Logger logger = LoggerFactory.getLogger(EmployeeDataMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        if (fields.length > 12 && !"DOB".equals(fields[12])) {
            Employee employee = new Employee();
            employee.setEmployeeName(fields[0]);
            employee.setEmployeeId(fields[1]);
            employee.setEmployeeDob(fields[12]);
            employee.setSex(fields[13]);
            employee.setMaritalStatus(fields[14]);
            employee.setEmployeeStatus(fields[22]);

            String[] dtFields = fields[12].split("/");
            context.write(new IntWritable(Integer.parseInt(dtFields[0]) - 1), employee);
        }
    }
}
