package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BloomFilterGenerationReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {
    @Override
    public void reduce(IntWritable rating, Iterable<BloomFilter> bloomFilters, Context context) throws IOException, InterruptedException {
        BloomFilter finalBf = null;

        //System.out.println("RATING " + rating.get());
        for (BloomFilter bf: bloomFilters) {
            //System.out.println("BF (from mapper): " + bf);
            if(finalBf == null) {
                finalBf = new BloomFilter(bf.getM(), bf.getK());
                finalBf.setNumElem(bf.getNumElem());
                finalBf.setBits(bf.getBits());
                continue;
            }
            finalBf.or(bf);
            finalBf.setNumElem(finalBf.getNumElem() + bf.getNumElem());
            //System.out.println("BF FINAL DOPO: " + finalBf);
        }

        //System.out.println("===============================");
        context.write(rating, finalBf);
    }
}
