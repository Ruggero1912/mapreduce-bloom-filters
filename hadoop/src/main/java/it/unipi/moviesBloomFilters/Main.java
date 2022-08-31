package it.unipi.moviesBloomFilters;

import it.unipi.moviesBloomFilters.job1.DatasetCountInMapperCombiner;
import it.unipi.moviesBloomFilters.job1.DatasetCountReducer;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationMapper;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationReducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import it.unipi.moviesBloomFilters.job3.FiltersTestInMapperCombiner;
import it.unipi.moviesBloomFilters.job3.FiltersTestReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class Main {

    private static int N_LINES;
    private static long startTime;
    private static long stopTime;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <input file> <output> <lines per mapper>");
            System.exit(1);
        }

        System.out.println("<input> = " + otherArgs[0]);
        System.out.println("<output> = " + otherArgs[1]);
        System.out.println("<lines per mapper> = " + otherArgs[2]);

        N_LINES = Integer.parseInt(args[2]);
        startTime = System.currentTimeMillis();
        Job1(conf, args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB1:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");

        startTime = System.currentTimeMillis();
        Job2(conf, args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB2:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");

        startTime = System.currentTimeMillis();
        Job3(args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB3:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");

        //count false positive rate
        Path path = new Path("hdfs://hadoop-namenode:9820/user/hadoop/" + args[1] + "_3/part-r-00000");
        HashMap<Integer, Double> fp_rates = BloomFilterUtility.countFalsePositiveRate(path);
        System.out.println("fp_rates:");
        System.out.println(Arrays.asList(fp_rates));
        System.exit(0);
    }


    private static void Job1(Configuration conf, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance(conf, "counter of films per rating");
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

        Boolean countSuccess1 = job1.waitForCompletion(true);
        if(!countSuccess1) {
            System.exit(0);
        }
    }

    public static void Job2(Configuration conf, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        Job job2 = Job.getInstance(conf, "Bloom Filter Generation");
        job2.setJarByClass(Main.class);

        // Set mapper/reducer
        job2.setMapperClass(BloomFilterGenerationMapper.class);
        job2.setReducerClass(BloomFilterGenerationReducer.class);

        // Mapper's output key-value
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(BloomFilter.class);

        // Reducer's output key-value
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(BloomFilter.class);

        // Define I/O
        NLineInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_2"));

        job2.setInputFormatClass(NLineInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class); // stores data in serialized key-value pair

        // Configuration parameters
        job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/" + args[1] + "/");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(pt);
        for (FileStatus fileStatus : status) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                int n, m, k, rating;
                double p;

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                for(Iterator<String> it = br.lines().iterator(); it.hasNext();) {
                    String[] tokens = it.next().split("\t");

                    rating = Integer.parseInt(tokens[0]);
                    n = Integer.parseInt(tokens[1]);

                    // Computing filter parameters
                    if (n != 0) {
                        p = BloomFilterUtility.getP(n);
                        m = BloomFilterUtility.getSize(n, p);
                        k = BloomFilterUtility.getNumberHashFunct(m, n);

                        // Passing parameters to the mapper for each filter
                        job2.getConfiguration().setInt("bf." + (rating - 1) + ".parameter.m", m);
                        job2.getConfiguration().setInt("bf." + (rating - 1) + ".parameter.k", k);
                    }
                }
                br.close();
                fs.close();
            }
        }

        Boolean countSuccess2 = job2.waitForCompletion(true);
        if(!countSuccess2) {
            System.exit(0);
        }
    }

    private static void Job3(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf3 = new Configuration();

        Job job3 = Job.getInstance(conf3, "False Positive Rate Evaluation");
        job3.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job3, new Path(args[0]));
        job3.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        job3.setJarByClass(Main.class);
        job3.setMapperClass(FiltersTestInMapperCombiner.class);
        job3.setReducerClass(FiltersTestReducer.class);

        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);

        String filename = "hdfs://hadoop-namenode:9820/user/hadoop/" + args[1] + "_2/part-r-00000";
        job3.addCacheFile(new Path(filename).toUri());

        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_3"));
        Boolean countSuccess3 = job3.waitForCompletion(true);
        if(!countSuccess3) {
            System.exit(0);
        }
    }
}
