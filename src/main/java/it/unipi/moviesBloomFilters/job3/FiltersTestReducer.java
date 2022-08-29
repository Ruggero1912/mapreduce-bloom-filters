package it.unipi.moviesBloomFilters.job3;

import it.unipi.moviesBloomFilters.BloomFilter;
import it.unipi.moviesBloomFilters.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FiltersTestReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable> {

    private static final int N_FILTERS = 10;

    public void reduce(IntWritable rating, Iterable<IntArrayWritable> counts, Context ctx) throws IOException, InterruptedException {
        int counterFP = 0;
        int counterFN = 0;
        int counterTP = 0;
        int counterTN = 0;

        System.out.println("REDUCER FOR RATING " + rating);

        for (IntArrayWritable count: counts) {
            Integer[] countsArray = count.toArray();
            for (int i = 0; i < countsArray.length; i++) {
                counterFP += countsArray[0];
                counterFN += countsArray[1];
                counterTP += countsArray[2];
                counterTN += countsArray[3];
            }
        }

        System.out.println("FP: " + counterFP + " | FN: " + counterFN + " | TP: " + counterTP + " | TN: " + counterTN);

        Integer[] finalCounts = {counterFP, counterFN, counterTP, counterTN};
        ctx.write(rating, new IntArrayWritable(finalCounts));
    }
}
