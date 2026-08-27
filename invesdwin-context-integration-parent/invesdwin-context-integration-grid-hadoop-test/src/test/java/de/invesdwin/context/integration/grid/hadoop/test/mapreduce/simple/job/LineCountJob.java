package de.invesdwin.context.integration.grid.hadoop.test.mapreduce.simple.job;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Immutable
public class LineCountJob {

    public static class LineCountMapper
            extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text("total_lines");

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            // Emits exactly one count per line of text read
            context.write(word, ONE);
        }
    }

    public static class LineCountReducer
            extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}