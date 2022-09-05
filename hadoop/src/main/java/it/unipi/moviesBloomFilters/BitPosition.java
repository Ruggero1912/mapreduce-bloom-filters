package it.unipi.moviesBloomFilters;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class BitPosition implements Writable, Serializable {
    private int[] indexes;

    public BitPosition() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(indexes.length);
        for(int i = 0; i < indexes.length; i++)
            dataOutput.writeInt(indexes[i]);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int len = dataInput.readInt();
        this.indexes = new int[len];
        for (int i = 0; i < len; i++)
            this.indexes[i] = dataInput.readInt();
    }

    public int[] getIndexes() {
        return indexes;
    }

    public void setIndexes(int[] indexes) {
        this.indexes = indexes;
    }
}
