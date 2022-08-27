package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
    private BloomFilter[] bfArray;
    private final int ratings = 10;

    @Override
    public void setup(Context context) {
        bfArray = new BloomFilter[ratings];

        // READ PARAMETERS FROM HDFS
        int m = 0;
        int k = 0;

        for (int i = 0; i < ratings; i++) {
            if (m == 0 || k == 0)
                bfArray[i] = null;
            else
                bfArray[i] = new BloomFilter(m, k);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws NumberFormatException  {
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        // Example of record: tt9916544  6.8   57
        String[] tokens = record.trim().split("\\s* \\s*");
        if(tokens.length == 3) {
            int roundedRating = Math.round(Float.parseFloat(tokens[1]));

            if(roundedRating != 0)
                bfArray[roundedRating - 1].add(tokens[0]);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < ratings; i++)
            if(bfArray[i] != null)
                context.write(new IntWritable(i + 1), bfArray[i]);
    }
}
