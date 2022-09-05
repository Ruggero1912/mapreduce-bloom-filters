package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// In this implementation for each record in the dataset the mapper emits (roundedRating, BloomFilter).
// The reducer is the same of the version 1.
// This is the most inefficient implementation.

public class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
    private final IntWritable reducerKey = new IntWritable();
    private BloomFilter reducerValue = new BloomFilter();

    @Override
    public void map(Object key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {

        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        String[] tags = value.toString().split("\t");
        if(tags.length != 3)
            return;

        int roundedRating = Math.round(Float.parseFloat(tags[1]));
        if (roundedRating == 0)
            return;

        // Creating one bloom filter per movie
        int m = context.getConfiguration().getInt("bf." + (roundedRating - 1) + ".parameter.m", 0);
        int k = context.getConfiguration().getInt("bf." + (roundedRating - 1) + ".parameter.k", 0);

        if (m != 0 && k != 0) {
            reducerValue.reset(m, k);
            reducerValue.add(tags[0]);

            reducerKey.set(roundedRating);
            context.write(reducerKey, reducerValue);
        }
    }
}