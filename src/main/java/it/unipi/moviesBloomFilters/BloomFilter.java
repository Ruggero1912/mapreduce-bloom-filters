package it.unipi.moviesBloomFilters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

public class BloomFilter implements Writable {
    private BitSet bits;
    private int m;
    private int k;
    // k hash function Murmur Hash

    /**
     * instantiate a new empty Bloom filter
     * @param m is the vector size
     * @param k is the number of hash functions to exploit for the Bloom filter
     */
    public BloomFilter(int m, int k){
        this.m = m;
        this.k = k;
        this.bits = new BitSet(m);
    }

    /**
     * adds an item to the Bloom filter
     * @param item
     */
    public void add(String item) {
        // iterates over the given number of hash functions (k), and
        // for each it sets one bit in the Bloom filter bits array
        for (int i = 0; i < this.k; i++) {
            //String id = item.replace("t", "0");
            int digestIndex = (int) ((MurmurHash.getInstance().hash(item.getBytes(), i)) % this.m);
            this.bits.set(digestIndex);
        }

    }

    /**
     * checks if the given item is supposed to be in set for the Bloom Filter
     * @param item the item to check
     * @return true if the item is supposed to be in set, else false
     */
    public boolean check(String item) {
        for (int i = 0; i < this.k; i++) {
            //String id = item.replace("t", "0");
            int digestIndex = (int) ((MurmurHash.getInstance().hash(item.getBytes(), i)) % this.m);
            if (!this.bits.get(digestIndex)) {
                return false;
            }
        }
        return true;
    }

    public String toString(){
        return this.bits.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        long[] longs = this.bits.toLongArray();
        dataOutput.writeInt(m);
        dataOutput.writeInt(k);
        dataOutput.writeInt(longs.length);
        for (int i = 0; i < longs.length; i++) {
            dataOutput.writeLong(longs[i]);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.m = dataInput.readInt();
        this.k = dataInput.readInt();
        long[] longs = new long[dataInput.readInt()];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = dataInput.readLong();
        }
        this.bits = BitSet.valueOf(longs);
    }
}