package it.unipi.moviesBloomFilters.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DatasetCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable rating, Iterable<IntWritable> counts, Context ctx) throws IOException, InterruptedException {
        int n = 0;
        for (IntWritable count:
             counts) {
            n += count.get();
        }
        ctx.write(rating, new IntWritable(n));
    }
}