package it.unipi.moviesBloomFilters;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class BitPosition implements Writable, Serializable {
    private int[] pos;

    public BitPosition() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pos.length);
        for(int i = 0; i < pos.length; i++)
            dataOutput.writeInt(pos[i]);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int len = dataInput.readInt();
        this.pos = new int[len];
        for (int i = 0; i < len; i++)
            this.pos[i] = dataInput.readInt();
    }

    public int[] getPos() {
        return pos;
    }

    public void setPos(int[] pos) {
        this.pos = pos;
    }
}
