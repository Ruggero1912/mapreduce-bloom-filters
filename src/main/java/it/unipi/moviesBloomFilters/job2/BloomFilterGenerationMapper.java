package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import it.unipi.moviesBloomFilters.BloomFilterUtility;
import it.unipi.moviesBloomFilters.MovieRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kerby.config.Config;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
    private BloomFilter[] bfArray;
    private final int ratings = 10;

    @Override
    public void setup(Context context) {
        bfArray = new BloomFilter[ratings];

        // Reading parameters from HDFS
        int m, k, n;

        for (int i = 0; i < ratings; i++) {
            n = context.getConfiguration().getInt("filter." + i + ".parameter.n", 0);
            if ( n != 0) {
                double p = BloomFilterUtility.getP(n);
                m = BloomFilterUtility.getSize(n, p);
                k = BloomFilterUtility.getNumberHashFunct(m, n);

                if (m != 0 && k != 0)
                    bfArray[i] = new BloomFilter(m, k);
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws NumberFormatException  {
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        MovieRow row = BloomFilterUtility.parseRow(record);
        if (row != null) {
            System.out.println("MovieID" + row.getMovieID() + " | Rounded Rating: " + row.getRoundedRating());
            System.out.println("Array length" + bfArray.length);

            bfArray[row.getRoundedRating() - 1].add(row.getMovieID());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < ratings; i++)
            if(bfArray[i] != null)
                context.write(new IntWritable(i + 1), bfArray[i]);
    }
}
