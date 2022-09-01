package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
    private static final int ratings = 10;
    private final BloomFilter[] bfArray = new BloomFilter[ratings];
    private final IntWritable key = new IntWritable();

    @Override
    public void setup(Context context) {
        int m, k;

        for (int i = 0; i < ratings; i++) {
            m = context.getConfiguration().getInt("bf." + i + ".parameter.m", 0);
            k = context.getConfiguration().getInt("bf." + i + ".parameter.k", 0);

            if (m != 0 && k != 0)
                bfArray[i] = new BloomFilter( m, k);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws NumberFormatException  {
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        String[] tags = value.toString().split("\t");
        if(tags.length != 3)
            return;

        int roundedRating = Math.round(Float.parseFloat(tags[1]));
        if (roundedRating == 0)
            return;

        //System.out.println("MovieID " + row.getMovieID() + " | Rounded Rating: " + row.getRoundedRating());
        bfArray[roundedRating - 1].add(tags[0]);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < ratings; i++)
            if (bfArray[i] != null && !bfArray[i].getBits().isEmpty()) {
                //System.out.println("[" + (i + 1) + "] Sending " + bfArray[i].toString());
                key.set(i + 1);
                context.write( key, bfArray[i]);
            }
    }
}
