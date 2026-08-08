package de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped.job.HadoopTestJobMapper;
import de.invesdwin.context.integration.grid.hadoop.test.mapreduce.bootstrapped.job.HadoopTestJobReducer;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
@Testcontainers
public class BootstappedMapReduceTest extends ATest {

    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void test() throws Exception {
        // 1. Prepare Input Data on HDFS via IFileChannel
        final IFileChannel fileChannel = FileChannelRegistry.newInstance(HADOOP.getHdfsUri());
        final IFileChannel inputFileChannel = fileChannel.withAbsolutePath("/tmp/test-input/data.txt");

        // Write a simple text file to HDFS cleanly
        inputFileChannel.uploadString("one\ntwo\nthree");

        // 2. Configure the Job
        final Configuration conf = HADOOP.newHadoopConfiguration();
        final Job job = Job.getInstance(conf, "Spring Integration Test");

        job.setJar(new MergedClasspathJar(HadoopMergedClasspathJarFilter.HADOOP3).getResource()
                .getFile()
                .getAbsolutePath());

        job.setMapperClass(HadoopTestJobMapper.class);
        job.setReducerClass(HadoopTestJobReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Resolve input and output directory paths using withAbsoluteDirectory
        final IFileChannel inputDirChannel = fileChannel.withAbsoluteDirectory("/tmp/test-input");
        FileInputFormat.addInputPath(job, new Path(inputDirChannel.getDirectoryUri()));
        FileOutputFormat.setOutputPath(job, new Path(HADOOP.getHdfsUri() + "/tmp/test-output"));

        // 3. Submit and wait for completion
        final boolean success = job.waitForCompletion(true);

        // 4. Assert the job succeeded
        Assertions.checkTrue(success, "MapReduce job failed on YARN!");

        // 5. Find the part file (MapReduce creates files like part-r-00000) using IFileChannel listing
        final IFileChannel outputChannel = fileChannel.withAbsoluteDirectory("/tmp/test-output");

        final IFileInfo resultFileInfo = outputChannel.listFiles()
                .stream()
                .filter(file -> file.getFilename().startsWith("part-"))
                .findFirst()
                .orElse(null);

        // 6. Assert file exists
        Assertions.checkTrue(resultFileInfo != null, "No output file found!");

        // 7. Read and assert the content using downloadString()
        final String text = outputChannel.withFilename(resultFileInfo.getFilename()).downloadString();
        final String expectedText = "0_mapped_reduced\t[one_mapped]_reduced\n"
                + "4_mapped_reduced\t[two_mapped]_reduced\n" + "8_mapped_reduced\t[three_mapped]_reduced\n";
        Assertions.assertThat(text).isEqualTo(expectedText);
    }
}