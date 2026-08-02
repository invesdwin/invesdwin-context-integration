package de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.grid.hadoop.test.HadoopContainer;
import de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped.job.HadoopTestJobMapper;
import de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped.job.HadoopTestJobReducer;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
@Testcontainers
public class BootstappedMapReduceTest extends ATest {

    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void test() throws Exception {
        //        Assertions.assertThat(fsh.test(new SystemProperties().getString("hadoopTestJob.input.path"))).isTrue();
        //        try {
        //            hadoopTestJobRunner.call();
        //            Assertions.assertThat(fsh.test(new SystemProperties().getString("hadoopTestJob.output.path") + "/_SUCCESS"))
        //                    .isTrue();
        //            final Collection<String> textCol = fsh
        //                    .text(new SystemProperties().getString("hadoopTestJob.output.path") + "/part-r-00000");
        //            Assertions.assertThat(textCol.size()).isEqualTo(1);
        //            final String text = textCol.iterator().next();
        //        } finally {
        //            Assertions.assertThat(cleanup.call()).isNull();
        //        }

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
            out.write("one\ntwo\nthree".getBytes(StandardCharsets.UTF_8));
        }

        // 2. Configure the Job
        final Job job = Job.getInstance(conf, "Spring Integration Test");

        job.setJar(new MergedClasspathJar(MergedClasspathJarFilter.HADOOP3).getResource().getFile().getAbsolutePath());

        job.setMapperClass(HadoopTestJobMapper.class);
        job.setReducerClass(HadoopTestJobReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        try (FSDataInputStream in = fs.open(resultFile)) {
            final String text = IOUtils.toString(in, Charset.defaultCharset());
            final String expectedText = "0_mapped_reduced\t[one_mapped]_reduced\n" //
                    + "4_mapped_reduced\t[two_mapped]_reduced\n" //
                    + "8_mapped_reduced\t[three_mapped]_reduced\n";
            Assertions.assertThat(text).isEqualTo(expectedText);
        }
    }
}
