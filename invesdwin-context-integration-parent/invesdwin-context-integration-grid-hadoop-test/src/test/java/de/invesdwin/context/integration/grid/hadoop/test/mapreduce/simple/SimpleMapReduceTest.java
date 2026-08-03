package de.invesdwin.context.integration.grid.hadoop.test.mapreduce.simple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.grid.hadoop.HadoopMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.hadoop.test.HadoopContainer;
import de.invesdwin.context.integration.grid.hadoop.test.mapreduce.simple.job.LineCountJob;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
@Testcontainers
public class SimpleMapReduceTest extends ATest {

    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void test() throws Exception {
        final Configuration conf = HADOOP.newHadoopConfiguration();
        final FileSystem fs = FileSystem.get(conf);

        // 1. Prepare Input Data on HDFS
        final Path inputPath = new Path("/tmp/test-input/data.txt");
        final Path outputPath = new Path("/tmp/test-output");

        // Clean up previous runs
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Write a simple file to HDFS
        try (OutputStream out = fs.create(inputPath)) {
            out.write("Line 1\nLine 2\nLine 3".getBytes(StandardCharsets.UTF_8));
        }

        // 2. Configure the Job
        final Job job = Job.getInstance(conf, "Line Count Integration Test");

        job.setJar(new MergedClasspathJar(HadoopMergedClasspathJarFilter.HADOOP3).getResource()
                .getFile()
                .getAbsolutePath());

        job.setMapperClass(LineCountJob.LineCountMapper.class);
        job.setCombinerClass(LineCountJob.LineCountReducer.class);
        job.setReducerClass(LineCountJob.LineCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath.getParent());
        FileOutputFormat.setOutputPath(job, outputPath);

        // 3. Submit and wait for completion
        final boolean success = job.waitForCompletion(true);

        // 4. Assert the job succeeded
        Assertions.checkTrue(success, "MapReduce job failed on YARN!");

        // 5. Find the part file (MapReduce creates files like part-r-00000)
        final FileStatus[] fileStatuses = fs.listStatus(outputPath);
        Path resultFile = null;
        for (final FileStatus status : fileStatuses) {
            if (status.getPath().getName().startsWith("part-")) {
                resultFile = status.getPath();
                break;
            }
        }

        // 6. Assert file exists
        Assertions.checkTrue(resultFile != null, "No output file found!");

        // 7. Read and assert the content
        try (FSDataInputStream in = fs.open(resultFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            final String line = reader.readLine();
            // Assuming our mapper emitted "total_lines" and we expect 3 lines of input
            Assertions.checkTrue(line != null && line.contains("total_lines\t3"),
                    "Expected output 'total_lines 3', but got: " + line);
        }
    }

}