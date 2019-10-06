package jeevkulk.mapreduce.practice.average.wordaveragelength;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordAverageLengthPartitioner extends Partitioner<Text, WordStats> {

    @Override
    public int getPartition(Text startingChar, WordStats wordStats, int numberOfPartitions) {
        return startingChar.toString().toCharArray()[0] % 97;
    }
}
