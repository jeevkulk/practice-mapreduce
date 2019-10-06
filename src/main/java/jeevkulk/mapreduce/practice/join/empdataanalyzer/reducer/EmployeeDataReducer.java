package jeevkulk.mapreduce.practice.join.empdataanalyzer.reducer;

import jeevkulk.mapreduce.practice.join.empdataanalyzer.domain.Employee;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EmployeeDataReducer extends Reducer<IntWritable, Employee, IntWritable, Text> {

    public enum Counter {
        MARRIED_EMPLOYEES,
        FEMALE_EMPLOYEES,
        TERMINATED_EMPLOYEES,
        ALL_EMPLOYEES
    }

    @Override
    protected void reduce(IntWritable monthOfBirthMapperKey, Iterable<Employee> employees, Context context) throws IOException, InterruptedException {
        /**
         * Below block gives number of birthdays on each day of the month. Note: files are split month-wise
         */
        /*int[] count = new int[31];
        for (Employee employee : employees) {
            count[Integer.parseInt(employee.getEmployeeDob().split("/")[1]) - 1] ++;
        }
        for (int i = 0; i < count.length; i++) {
            context.write(new IntWritable(i + 1), new Text(Integer.toString(count[i])));
        }*/
        /**
         * Below block gives month and employee details in month-wise split files
         */
        /*for (Employee employee : employees) {
            context.write(monthOfBirthMapperKey, new Text(employee.toString()));
        }*/

        /**
         * Below block gives number of birthdays for each month. Note: files are split month-wise
         */
        /*int count = 0;
        for (Employee employee : employees) {
            count ++;
        }
        context.write(monthOfBirthMapperKey, new Text(Integer.toString(count)));*/

        /**
         * Below block gives number of married people in the firm. Note: files are split month-wise as per birth-month
         */
        /*int count = 0;
        for (Employee employee : employees) {
            if ("Married".equals(employee.getMaritalStatus())) {
                count++;
            }
        }
        context.write(monthOfBirthMapperKey, new Text(Integer.toString(count)));*/
        for (Employee employee : employees) {
            if ("Married".equals(employee.getMaritalStatus())) {
                context.getCounter(Counter.MARRIED_EMPLOYEES).increment(1);
            }
            if ("Female".equals(employee.getSex())) {
                context.getCounter(Counter.FEMALE_EMPLOYEES).increment(1);
            }
            if (employee.getEmployeeStatus().indexOf("Terminated") != -1) {
                context.getCounter(Counter.TERMINATED_EMPLOYEES).increment(1);
            }
            context.getCounter(Counter.ALL_EMPLOYEES).increment(1);
        }
        context.write(monthOfBirthMapperKey, new Text(Long.toString(context.getCounter(Counter.MARRIED_EMPLOYEES).getValue())));
    }
}
