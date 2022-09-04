package it.unipi.moviesBloomFilters.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;

// This implementation is equal to the "BloomFilterGenerationMapper2" but the positions are concatenated
// and emitted as a string (inefficient)

public class BloomFilterGenerationMapper3 extends Mapper<Object, Text, IntWritable, Text> {
    private final IntWritable reducerKey = new IntWritable();
    private Text reducerValue = new Text();

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
            StringBuilder bits = new StringBuilder();
            for (int i = 0; i < k; i++) {
                bits.append(Math.abs(MurmurHash.getInstance().hash(tags[0].getBytes(), i)) % m);
                if (i != k - 1)
                    bits.append(",");
            }

            reducerKey.set(roundedRating);
            reducerValue.set(bits.toString());
            context.write(reducerKey, reducerValue);
        }
    }
}
