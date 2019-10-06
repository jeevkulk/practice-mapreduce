package jeevkulk.mapreduce.practice.average.wordaveragelength;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordAverageLengthMapper extends Mapper<LongWritable, Text, Text, WordStats> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String splitDataArr[] = value.toString().split("\\s+");
        for (String splitData : splitDataArr) {
            String word = splitData.replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!word.isEmpty())
                context.write(new Text(word.substring(0, 1)), new WordStats(word.length(), 1));
            else
                continue;
        }
    }
}
