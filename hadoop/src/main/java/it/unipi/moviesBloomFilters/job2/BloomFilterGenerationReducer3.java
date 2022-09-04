package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.BitSet;

public class BloomFilterGenerationReducer3 extends Reducer<IntWritable, Text, IntWritable, BloomFilter> {
    BloomFilter bf = new BloomFilter();

    @Override
    public void reduce(IntWritable rating, Iterable<Text> bits, Context context) throws IOException, InterruptedException {
        int m = context.getConfiguration().getInt("bf." + (rating.get() - 1) + ".parameter.m", 0);
        int k = context.getConfiguration().getInt("bf." + (rating.get() - 1) + ".parameter.k", 0);

        if (m == 0 || k == 0)
            return;

        BitSet bitSet = new BitSet(m);
        String[] tags;
        for (Text bitPos: bits) {
            tags = bitPos.toString().split(",");
            if(tags.length != 0) {
                // tags maximum length will be equal to k
                for (String tag : tags)
                    bitSet.set(Integer.parseInt(tag.toString()), true);
            }
        }

        if(!bitSet.isEmpty()) {
            bf.reset(m, k);
            bf.setBits(bitSet);
            context.write(rating, bf);
        }
    }
}
