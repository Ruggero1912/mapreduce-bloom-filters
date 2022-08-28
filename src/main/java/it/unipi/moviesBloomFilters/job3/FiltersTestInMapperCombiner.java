package it.unipi.moviesBloomFilters.job3;

import it.unipi.moviesBloomFilters.BloomFilter;
import it.unipi.moviesBloomFilters.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class FiltersTestInMapperCombiner extends Mapper<Object, Text, IntWritable, IntArrayWritable> {

    private static final String  BLOOM_FILTERS_PATH = "";
    private static final int N_FILTERS = 10;

    private BloomFilter[] bloomFilters = new BloomFilter[N_FILTERS];
    private int[] counterFP = new int[N_FILTERS];
    private int[] counterFN = new int[N_FILTERS];
    private int[] counterTP = new int[N_FILTERS];
    private int[] counterTN = new int[N_FILTERS];

    public void setup(Context ctx) throws IOException {
        URI[] cachedFiles = ctx.getCacheFiles();
        for (URI fileStatus : cachedFiles) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {

                SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(fileStatus.getPath())));

                //SequenceFile.Reader reader = new SequenceFile.Reader(new InputStreamReader(fs.open(new Path(fileStatus.getPath()))));
                boolean hasNext;

                do {
                    IntWritable key = new IntWritable();
                    BloomFilter filter = new BloomFilter();
                    hasNext = reader.next(key, filter);
                    if(key == null || filter == null || filter.getK() == 0){
                        continue;
                    }
                    System.out.println("ho letto chiave: " + key.get() + " | bloom filter: " + filter.toString());
                    this.bloomFilters[key.get() - 1] = filter;
                } while (hasNext);
            }
        }
    }

    public void map(Object key, Text value, Context context){

        /* value is a row of the dataset, it represents a movie */
        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");

        String movieID = itr.nextToken();
        int roundedRating = Math.round(Float.parseFloat(itr.nextToken()));

        int i = 1;
        for (BloomFilter bloomFilter:
             bloomFilters) {

            if(bloomFilter.check(movieID)){
                if (i == roundedRating)
                    counterTP[i-1] += 1;
                else
                    counterFP[i-1] += 1;
            } else{
                if (i != roundedRating)
                    counterTN[i-1] += 1;
                else
                    counterFN[i-1] += 1;
            }
            i += 1;
        }
    }

    public void cleanup(Context ctx) throws IOException, InterruptedException {
        for (int i = 0; i < N_FILTERS; i++) {
            Integer[] stats = {counterFP[i-1], counterFN[i-1], counterTP[i-1], counterTN[i-1]};
            ctx.write(  new IntWritable(i), new IntArrayWritable(stats));
        }
    }
}
