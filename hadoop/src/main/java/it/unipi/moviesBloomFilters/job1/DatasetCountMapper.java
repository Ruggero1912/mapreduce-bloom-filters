package it.unipi.moviesBloomFilters.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DatasetCountMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    private final IntWritable reducerKey = new IntWritable();
    private final IntWritable one = new IntWritable(1);

    // Mapper version without In-Mapper Combiner
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException, NumberFormatException {

        // Dataset row example: tt9916544  6.8  57
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        String[] tags = value.toString().split("\t");
        if(tags.length != 3)
            return;

        int roundedRating = Math.round(Float.parseFloat(tags[1]));
        if (roundedRating == 0)
            return;

        reducerKey.set(roundedRating);
        context.write(reducerKey, one);
    }
}