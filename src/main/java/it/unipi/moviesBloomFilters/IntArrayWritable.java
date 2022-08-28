package it.unipi.moviesBloomFilters;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

    private IntWritable[] writableValues;

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(Integer[] values) {
        super(IntWritable.class);
        writableValues = new IntWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            writableValues[i] = new IntWritable(values[i]);
        }
        set(writableValues);
    }

    public Integer[] toArray(){
        Integer[] arrayValues = new Integer[this.writableValues.length];
        for (int i = 0; i < this.writableValues.length; i++) {
            arrayValues[i] = this.writableValues[i].get();
        }
        return arrayValues;
    }
}
