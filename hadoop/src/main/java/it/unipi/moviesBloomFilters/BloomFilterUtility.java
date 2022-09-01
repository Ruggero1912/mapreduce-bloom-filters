package it.unipi.moviesBloomFilters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.IntStream;

public class BloomFilterUtility {
    public static final int ratings = 10;
    public static int datasetSize;
    public static int[] sizes = new int[ratings];

    public static void getDataset_size(Path path){
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(path);

            for (FileStatus fileStatus : status) {
                if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                    int rating, size;
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                    for(Iterator<String> it = br.lines().iterator(); it.hasNext();) {
                        String[] tokens = it.next().split("\t");

                        //line = (rating: IntWritable, n: IntWritable)
                        rating = Integer.parseInt(tokens[0]);
                        size = Integer.parseInt(tokens[1]);
                        sizes[rating-1] = size;
                    }
                }
            }

            datasetSize = IntStream.of(sizes).sum();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    public static double getP(int n, int dataset_size){
        double p = 0;
        float perc=((float)n/dataset_size)*100;
        if(perc<=2){
            p=0.1;
        }
        else if (perc>2 && perc<=15){
            p=0.01;
        }
        else if (perc>15){
            p=0.001;
        }
        return p;
    }

    public static int getSize(int n, double p){
        return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
    }

    public static int getNumberHashFunct(int size, int n){
        return (int) ((size / n) * Math.log(2));
    }

    public static MovieRow parseRow(String value) {
        StringTokenizer itr = new StringTokenizer(value, "\t");
        if(itr.countTokens() != 3)
            return null;

        String movieID = itr.nextToken();
        int roundedRating = Math.round(Float.parseFloat(itr.nextToken()));
        if (roundedRating == 0)
            return null;

        return new MovieRow(movieID, roundedRating);
    }


    public static HashMap<Integer, Double> countFalsePositiveRate(Path path) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(path);
            // fp_rates(key = rating, value = fpr of the relative bloom filter)
            HashMap<Integer, Double> fp_rates= new HashMap<>();

            for (FileStatus fileStatus : status) {
                if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                    int rating;
                    String finalCounts;
                    double fpr;

                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    System.out.println("rating: FP,FN,TP,TN");
                    for (Iterator<String> it = br.lines().iterator(); it.hasNext(); ) {
                        String[] tokens = it.next().split("\t");

                        //line = (rating: IntWritable, finalCounts: Text(String FP,FN,TP,TN))
                        rating = Integer.parseInt(tokens[0]);
                        finalCounts = tokens[1];

                        System.out.println(rating +"\t"+finalCounts);

                        String[] counts = finalCounts.split(",");
                        double fp = Double.parseDouble(counts[0]);
                        double tn = Double.parseDouble(counts[3]);
                        fpr = fp/(fp+tn);
                        fp_rates.put(rating, fpr);
                    }
                }
            }
            return fp_rates;
        }
        catch (Exception e) { e.printStackTrace(); }

        return null;

    }

    public static void setConfigurationParams(Job job) throws IOException {
        // Setting configuration parameters for job2
        int m, k, i = 0;
        double p;
        for (int size : sizes) {
            if (size != 0) {
                p = BloomFilterUtility.getP(size, datasetSize);
                m = BloomFilterUtility.getSize(size, p);
                k = BloomFilterUtility.getNumberHashFunct(m, size);

                System.out.println( (i+1) + " | m=" + m + ", k=" + k + ", p=" + p + ", n=" + size);

                // Passing parameters to the mapper for each filter
                job.getConfiguration().setInt("bf." + (i) + ".parameter.m", m);
                job.getConfiguration().setInt("bf." + (i) + ".parameter.k", k);
            }
            i++;
        }
    }
}
