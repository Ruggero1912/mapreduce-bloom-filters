package it.unipi.moviesBloomFilters.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DatasetCountReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final IntWritable sum = new IntWritable();

    public void reduce(IntWritable rating, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int n = 0;
        for (final IntWritable count: counts) {
            n += count.get();
        }
        sum.set(n);
        context.write(rating, sum);
    }
}