package it.unipi.moviesBloomFilters.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class FiltersTestReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable rating, Iterable<Text> counters, Context ctx) throws IOException, InterruptedException {
        int counterFP = 0;
        int counterFN = 0;
        int counterTP = 0;
        int counterTN = 0;
        int counterMultiPositiveResults = 0;

        for (Text stats: counters) {
            StringTokenizer itr = new StringTokenizer(stats.toString(), ",");
            if (itr.countTokens()==4){
                counterFP += Integer.parseInt(itr.nextToken());
                counterFN += Integer.parseInt(itr.nextToken());
                counterTP += Integer.parseInt(itr.nextToken());
                counterTN += Integer.parseInt(itr.nextToken());
            }
            else{
                counterMultiPositiveResults += Integer.parseInt(itr.nextToken());
            }
        }

        Text finalCounts =  new Text( String.valueOf(counterFP) + ',' +
                                        String.valueOf(counterFN) + ',' +
                                        String.valueOf(counterTP) + ',' +
                                        String.valueOf(counterTN));
        Text finalCountMultiPositiveResults = new Text(String.valueOf(counterMultiPositiveResults));
        if(rating.get()<11){
            ctx.write(rating, finalCounts);
        }
        else{
            ctx.write(rating, finalCountMultiPositiveResults);
        }
    }
}
