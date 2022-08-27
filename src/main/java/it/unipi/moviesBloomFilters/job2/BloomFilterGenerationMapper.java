package it.unipi.moviesBloomFilters.job2;

import it.unipi.moviesBloomFilters.BloomFilter;
import it.unipi.moviesBloomFilters.BloomFilterUtility;
import it.unipi.moviesBloomFilters.MovieRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

public class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
    private BloomFilter[] bfArray;
    private final int ratings = 10;
    static Logger log = Logger.getLogger(BloomFilterGenerationMapper.class.getName());

    @Override
    public void setup(Context context) {
        bfArray = new BloomFilter[ratings];

        // Reading parameters from HDFS
        int m, k;

        System.out.println("INIZIO LOG CON SYSTEMOUT");

        try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output-prova/part-r-00000"); // Location of file in HDFS
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(pt));
            boolean hasNext;
            do {
                IntWritable keyWritable = new IntWritable();
                IntWritable nWritable = new IntWritable();
                hasNext = reader.next(keyWritable, nWritable);

                int rating = keyWritable.get();
                int n = nWritable.get();
                double p = BloomFilterUtility.getP(n);
                m = BloomFilterUtility.getSize(n, p);
                k = BloomFilterUtility.getNumberHashFunct(m, n);

                System.out.println("m: " + m + " | k: " + k + " | n:" + n);

                if (m == 0 || k == 0)
                    throw new Exception("m or k are 0");
                else
                    bfArray[rating - 1] = new BloomFilter(m, k);

            } while(hasNext);

        } catch (Exception e) {
            e.getStackTrace();
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

            for (int i = 0; i < bfArray.length; i++) {
                if(bfArray[i] == null)
                    System.out.println("Index " + i + "  è vuoto");
                else
                    System.out.println(" " + bfArray[i]);
            }

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
