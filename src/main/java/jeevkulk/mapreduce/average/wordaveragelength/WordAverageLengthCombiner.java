package jeevkulk.mapreduce.average.wordaveragelength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordAverageLengthCombiner extends Reducer<Text, WordStats, Text, WordStats> {

    public void reduce(Text wordKey, Iterable<WordStats> wordStats, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (WordStats wordStat : wordStats) {
            count += wordStat.getCount();
            sum += wordStat.getSum();
        }
        context.write(wordKey, new WordStats(sum, count));
    }
}
