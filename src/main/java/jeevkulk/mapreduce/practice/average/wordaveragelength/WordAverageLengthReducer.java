package jeevkulk.mapreduce.practice.average.wordaveragelength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordAverageLengthReducer extends Reducer<Text, WordStats, Text, Text> {

    public void reduce(Text startingChar, Iterable<WordStats> wordStats, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (WordStats wordStat : wordStats) {
            sum += wordStat.getSum();
            count += wordStat.getCount();
        }
        float avg = (sum / (float) count);
        context.write(new Text(startingChar), new Text(Integer.toString(sum) + " : " + Float.toString(avg)));
    }
}
