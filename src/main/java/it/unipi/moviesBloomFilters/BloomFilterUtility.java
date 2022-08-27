package it.unipi.moviesBloomFilters;
import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class BloomFilterUtility {
    public static int dataset_size= 1248408;

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
        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        if(itr.countTokens() != 3)
            return null;

        String movieID = itr.nextToken().toString();
        int roundedRating = Math.round(Float.parseFloat( itr.nextToken().toString() ) );
        if (roundedRating == 0)
            return null;

        return new MovieRow(movieID, roundedRating);
    }
}
