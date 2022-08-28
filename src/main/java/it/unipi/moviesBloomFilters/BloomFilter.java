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
    private int k; // k hash function Murmur Hash
    private int numElem; // elements in the filter

    /**
     * instantiate a new empty Bloom filter
     * @param m is the vector size
     * @param k is the number of hash functions to exploit for the Bloom filter
     */
    public BloomFilter(int m, int k){
        this.m = m;
        this.k = k;
        this.bits = new BitSet(m);
        this.numElem = 0;
    }

    public BloomFilter() {
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
            int digestIndex = (Math.abs(MurmurHash.getInstance().hash(item.getBytes(), i)) % this.m);
            //System.out.println("MovieId: " +  item + " | Hash [" + i + "] to index " +  digestIndex);
            this.bits.set(digestIndex, true);
        }
        this.numElem = this.numElem + 1;
    }

    /**
     * checks if the given item is supposed to be in set for the Bloom Filter
     * @param item the item to check
     * @return true if the item is supposed to be in set, else false
     */
    public boolean check(String item) {
        for (int i = 0; i < this.k; i++) {
            //String id = item.replace("t", "0");
            int digestIndex = (Math.abs(MurmurHash.getInstance().hash(item.getBytes(), i)) % this.m);
            if (!this.bits.get(digestIndex)) {
                return false;
            }
        }
        return true;
    }

    public void or(BloomFilter b) {
        this.bits.or(b.getBits());
    }

    //public String toString(){
    //    return this.bits.toString();
    //}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        long[] longs = this.bits.toLongArray();
        dataOutput.writeInt(m);
        dataOutput.writeInt(k);
        dataOutput.writeInt(numElem);
        dataOutput.writeInt(longs.length);
        for (int i = 0; i < longs.length; i++) {
            dataOutput.writeLong(longs[i]);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.m = dataInput.readInt();
        this.k = dataInput.readInt();
        this.numElem = dataInput.readInt();
        long[] longs = new long[dataInput.readInt()];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = dataInput.readLong();
        }
        this.bits = BitSet.valueOf(longs);
    }

    public BitSet getBits() {
        return bits;
    }

    public void setBits(BitSet bits) {
        this.bits = bits;
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    @Override
    public String toString() {
        return "BloomFilter{" +
                "bits=" + bits +
                ", m=" + m +
                ", k=" + k +
                ", numElem=" + numElem +
                '}';
    }

    public int getNumElem() {
        return numElem;
    }

    public void setNumElem(int numElem) {
        this.numElem = numElem;
    }
}