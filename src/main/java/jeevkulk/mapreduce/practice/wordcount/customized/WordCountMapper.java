package jeevkulk.mapreduce.practice.wordcount.customized;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String st [] = value.toString().split("\\s+");
        for(String st1 :  st) {
            context.write(new Text(st1.replaceAll("[^a-zA-Z]","").toLowerCase()), new IntWritable(1));
        }
	}
}
