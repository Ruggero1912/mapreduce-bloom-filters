package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BloomFilterGenerationReducer2 extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {
    @Override
    public void reduce(IntWritable rating, Iterable<BloomFilter> bloomFilters, Context context) throws IOException, InterruptedException {
        BloomFilter finalBf = null;

        for (BloomFilter bf: bloomFilters) {
            if(finalBf == null) {
                finalBf = new BloomFilter(bf.getM(), bf.getK(),bf.getBits());
                continue;
            }

            finalBf.or(bf);
        }

        //System.out.println("[EMIT] Rating: " + rating + "| BloomFilter: " + finalBf);
        context.write(rating, finalBf);
    }
}
