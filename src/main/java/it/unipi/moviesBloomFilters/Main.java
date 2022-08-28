package it.unipi.moviesBloomFilters;

import it.unipi.moviesBloomFilters.job1.DatasetCountInMapperCombiner;
import it.unipi.moviesBloomFilters.job1.DatasetCountReducer;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationMapper;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationReducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
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

        startTime= System.currentTimeMillis();
        Job2(conf1, otherArgs, args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB2:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");


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

    public static void Job2(Configuration conf, String[] otherArgs, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Job job2 = Job.getInstance(conf, "Bloom Filter Generation");
        job2.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job2, new Path(args[0]));
        job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", (N_LINES));
        job2.setJarByClass(Main.class);
        job2.setMapperClass(BloomFilterGenerationMapper.class);
        job2.setReducerClass(BloomFilterGenerationReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(BloomFilter.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(BloomFilter.class);

        Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/" + args[1] + "/");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(pt);
        for (FileStatus fileStatus : status) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                int n, index;
                for(Iterator<String> it = br.lines().iterator(); it.hasNext();) {
                    String[] tokens = it.next().split("\t");
                    index = Integer.parseInt(tokens[0]) - 1;
                    n = Integer.parseInt(tokens[1]);
                    //System.out.println("Rating: " + (index + 1) + " | n: " + n);

                    job2.getConfiguration().setInt("filter." + index + ".parameter.n", n);
                }
                br.close();
                fs.close();
            }
        }

        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_2"));
        Boolean countSuccess = job2.waitForCompletion(true);
        if(!countSuccess) {
            System.exit(0);
        }
    }
}
