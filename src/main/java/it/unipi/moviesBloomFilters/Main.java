package it.unipi.moviesBloomFilters;

import it.unipi.moviesBloomFilters.job1.DatasetCountInMapperCombiner;
import it.unipi.moviesBloomFilters.job1.DatasetCountReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Main {

    private static int N_LINES;
    private static long startTime;
    private static long stopTime;

    public static void main(String[] args) throws Exception {



        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: <input file> <output file> <lines per reducer>");
            System.exit(2);
        }

        N_LINES = Integer.parseInt(args[2]);
        startTime= System.currentTimeMillis();
        Job1(conf1, otherArgs, args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB1:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");


        System.exit(0);


    }


    private static void Job1(Configuration conf1, String[] otherArgs, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance(conf1, "counter of films per rating");
        job1.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job1, new Path(args[0]));
        job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", (N_LINES));
        job1.setJarByClass(Main.class);
        job1.setMapperClass(DatasetCountInMapperCombiner.class);
        job1.setReducerClass(DatasetCountReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        Boolean countSuccess = job1.waitForCompletion(true);
        if(!countSuccess) {
            System.exit(0);
        }


    }
}
