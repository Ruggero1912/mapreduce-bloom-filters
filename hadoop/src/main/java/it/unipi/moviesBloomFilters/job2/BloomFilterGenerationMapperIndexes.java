package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BitPosition;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;

// In this implementation for each record in the dataset the mapper emits (roundedRating, bit positions).
// Bit positions are the indexes that will be equal to true in the BitSet.
// In order to emit them efficiently a class called BitPosition has been implemented.

public class BloomFilterGenerationMapperIndexes extends Mapper<Object, Text, IntWritable, BitPosition> {
    private final IntWritable reducerKey = new IntWritable();
    private BitPosition reducerValue = new BitPosition();

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
            int[] bits = new int[k];
            for (int i = 0; i < k; i++) {
                bits[i] = Math.abs(MurmurHash.getInstance().hash(tags[0].getBytes(), i)) % m;
            }

            reducerKey.set(roundedRating);
            reducerValue.setPos(bits);
            context.write(reducerKey, reducerValue);
        }
    }
}
