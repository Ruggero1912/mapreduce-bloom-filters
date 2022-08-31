package it.unipi.moviesBloomFilters;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.StringTokenizer;


public class BloomFilterUtility {
    public static int dataset_size = 1252427;

    public static int getDataset_size(){
        //10 tuples (IntWritable rating, IntWritable n)
        return 1;
    }

    public static double getP(int n){
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


    public static Double countFalsePositiveRate(Path path) {
        Double fp = 0.0;
        try {

            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(path);

            for (FileStatus fileStatus : status) {
                if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                    int rating;
                    String finalCounts;
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

                    for (Iterator<String> it = br.lines().iterator(); it.hasNext(); ) {
                        String[] tokens = it.next().split("\t");

                        //line = (rating: IntWritable, finalCounts: Text(String FP+FN+TP+TN))
                        rating = Integer.parseInt(tokens[0]);
                        finalCounts = tokens[1];
                        System.out.println("finalCounts"+finalCounts);


                    }
                }
            }



        } catch (Exception e) { e.printStackTrace(); }

        return fp;

    }



}
