package it.unipi.moviesBloomFilters;

import it.unipi.moviesBloomFilters.job1.DatasetCountInMapperCombiner;
import it.unipi.moviesBloomFilters.job1.DatasetCountMapper2;
import it.unipi.moviesBloomFilters.job1.DatasetCountReducer;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationMapper;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationMapper2;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationReducer;
import it.unipi.moviesBloomFilters.job2.BloomFilterGenerationReducer2;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class Main {
    private static final String namenodePath = "hdfs://hadoop-namenode:9820/user/hadoop/";
    private static int N_LINES;
    private static long startTime;
    private static long stopTime;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: <input file> <output> <lines per mapper> <in-mapper implementation>");
            System.exit(1);
        }

        System.out.println("<input> = " + otherArgs[0]);
        System.out.println("<output> = " + otherArgs[1]);
        System.out.println("<lines per mapper> = " + otherArgs[2]);
        System.out.println("<in-mapper implementation> = " + Boolean.parseBoolean(otherArgs[3]));

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
        Job3(conf, args);
        stopTime = System.currentTimeMillis();
        System.out.println("Execution time JOB3:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");

        //count false positive rate
        Path path = new Path(namenodePath + args[1] + "_3/");
        HashMap<Integer, Double> fp_rates = BloomFilterUtility.countFalsePositiveRate(path);
        System.out.println("\nFalse positive rates:");
        fp_rates.forEach((key, value) -> System.out.println(key + " " + value));
        System.out.println("\nDataset size:" + BloomFilterUtility.datasetSize);
        System.out.println("\nTotal number of multi-positive results:" + BloomFilterUtility.counterMultiPositiveResults);

        double multi_positive_rates = ((double) BloomFilterUtility.counterMultiPositiveResults/
                (double) BloomFilterUtility.datasetSize)*100;
        System.out.println("\nMultiple positive rates: "+ String.format("%.4f",multi_positive_rates)+"%");
        System.exit(0);
    }


    private static void Job1(Configuration conf, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance(conf, "Counter of films per rating");
        job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", (N_LINES));
        job1.setJarByClass(Main.class);

        if (Boolean.parseBoolean(args[3])) {
            System.out.println("IN-MAPPER COMBINER VERSION");
            job1.setMapperClass(DatasetCountInMapperCombiner.class);
        } else {
            System.out.println("MAP + COMBINER VERSION");
            job1.setMapperClass(DatasetCountMapper2.class);
            job1.setCombinerClass(DatasetCountReducer.class);
        }
        job1.setReducerClass(DatasetCountReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        NLineInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(NLineInputFormat.class);

        Boolean countSuccess1 = job1.waitForCompletion(true);
        if(!countSuccess1) {
            System.exit(0);
        }
    }

    public static void Job2(Configuration conf, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        conf = new Configuration();
        Job job2 = Job.getInstance(conf, "Bloom Filter Generation");
        job2.setJarByClass(Main.class);

        // Set mapper/reducer
        if (Boolean.parseBoolean(args[3])) {
            System.out.println("IN-MAPPER COMBINER VERSION");
            job2.setMapperClass(BloomFilterGenerationMapper.class);
            job2.setReducerClass(BloomFilterGenerationReducer.class);

            // Mapper's output key-value (version 1)
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(BloomFilter.class);
        } else {
            System.out.println("MAP + COMBINER VERSION");
            job2.setMapperClass(BloomFilterGenerationMapper2.class);
            job2.setReducerClass(BloomFilterGenerationReducer2.class);

            // Mapper's output key-value (version 2)
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(BitPosition.class);
        }

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

        Path pt = new Path(namenodePath + args[1] + "/");
        BloomFilterUtility.getDatasetSize(pt);
        BloomFilterUtility.setConfigurationParams(job2);

        Boolean countSuccess2 = job2.waitForCompletion(true);
        if(!countSuccess2) {
            System.exit(0);
        }
    }

    private static void Job3(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job job3 = Job.getInstance(conf, "False Positive Rate Evaluation");
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

        String filename = namenodePath + args[1] + "_2/part-r-00000";
        job3.addCacheFile(new Path(filename).toUri());

        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_3"));
        Boolean countSuccess3 = job3.waitForCompletion(true);
        if(!countSuccess3) {
            System.exit(0);
        }
    }
}
