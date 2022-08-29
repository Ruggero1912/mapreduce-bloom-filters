package it.unipi.moviesBloomFilters;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public String toString() {
        IntWritable[] values = get();
        return values[0].toString() + ", " + values[1].toString();
    }
}
