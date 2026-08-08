package de.invesdwin.context.integration.grid.hadoop.test.mapreduce.simple;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
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

        // 1. Prepare Input Data on HDFS via IFileChannel
        final IFileChannel fileChannel = FileChannelRegistry.newInstance(HADOOP.getHdfsUri());
        final IFileChannel inputFileChannel = fileChannel.withAbsolutePath("/tmp/test-input/data.txt");

        // Write a simple file to HDFS cleanly
        inputFileChannel.uploadString("Line 1\nLine 2\nLine 3");

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

        final IFileChannel inputDirChannel = fileChannel.withAbsoluteDirectory("/tmp/test-input");
        final IFileChannel outputChannel = fileChannel.withAbsoluteDirectory("/tmp/test-output");

        FileInputFormat.addInputPath(job, new Path(inputDirChannel.getDirectoryUri()));
        FileOutputFormat.setOutputPath(job, new Path(outputChannel.getDirectoryUri()));

        // 3. Submit and wait for completion
        final boolean success = job.waitForCompletion(true);

        // 4. Assert the job succeeded
        Assertions.checkTrue(success, "MapReduce job failed on YARN!");

        // 5. Find the part file (MapReduce creates files like part-r-00000)
        final IFileInfo resultFileInfo = outputChannel.listFiles()
                .stream()
                .filter(file -> file.getFilename().startsWith("part-"))
                .findFirst()
                .orElse(null);

        // 6. Assert file exists
        Assertions.checkTrue(resultFileInfo != null, "No output file found!");

        // 7. Read and assert the content
        final String line = outputChannel.withFilename(resultFileInfo.getFilename()).downloadString();
        Assertions.checkTrue(line != null && line.contains("total_lines\t3"),
                "Expected output 'total_lines 3', but got: " + line);
    }
}