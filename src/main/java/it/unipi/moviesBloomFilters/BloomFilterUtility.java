package it.unipi.moviesBloomFilters;
import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class BloomFilterUtility {
    public static int dataset_size= 1248408;

    public static double getP(int n){
        double p = 0;
        float perc=((float)n/dataset_size)*100;
        System.out.println("perc "+perc);
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



}
