package it.unipi.moviesBloomFilters.job1;

import it.unipi.moviesBloomFilters.BloomFilterUtility;
import it.unipi.moviesBloomFilters.MovieRow;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class DatasetCountInMapperCombiner extends Mapper<Object, Text, IntWritable, IntWritable>{
    private int[] counters;
    private final IntWritable key = new IntWritable();
    private final IntWritable value = new IntWritable();

    public void setup(Context ctx){
        this.counters = new int[10];
        //Arrays.fill(this.counters, 0);
        // the default value for java array of type int is 0, so we do not need to initialize explicitly
    }
    public void map(Object key, Text value, Context ctx){
        // in value we have one row of the dataset, each line is made of:
        //  tt9916544<'\t'>6.8<'\t'>57<'\n'>
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        //MovieRow row = BloomFilterUtility.parseRow(record);
        String[] tags = value.toString().split("\t");
        //StringTokenizer itr = new StringTokenizer(value, "\t");
        if(tags.length != 3)
            return;

        int roundedRating = Math.round(Float.parseFloat(tags[1]));
        if (roundedRating == 0)
            return;

        this.counters[roundedRating - 1]++;
    }

    public void cleanup(Context ctx) throws IOException, InterruptedException {
        int i = 1;
        for (int c:
             this.counters) {
            key.set(i);
            value.set(c);
            ctx.write(key, value);
            i++;
        }
    }
}
