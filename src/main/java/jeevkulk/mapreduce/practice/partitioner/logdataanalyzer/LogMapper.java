package jeevkulk.mapreduce.practice.partitioner.logdataanalyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static List<String> months = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");

        if (fields.length > 3) {
            String ip = fields[0];
            String[] dtFields = fields[3].split("/");
            if (dtFields.length > 1) {
                String theMonth = dtFields[1];

                if (months.contains(theMonth))
                    context.write(new Text(ip), new Text(theMonth));
            }
        }
    }
}
