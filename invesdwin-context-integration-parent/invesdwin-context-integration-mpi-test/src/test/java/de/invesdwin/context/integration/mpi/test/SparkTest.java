package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.mpi.test.job.YarnJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class SparkTest extends ATest {

    @Test
    public void testSparkExecution() throws Exception {
        final int numContainers = 2;
        final File logDir = ContextProperties.getCacheDirectory();
        runSparkJob(numContainers, logDir.getAbsolutePath());

        final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");
        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }

    public static void runSparkJob(final int numContainers, final String logDirStr) {
        // Run locally with threads equal to container count
        final SparkConf conf = new SparkConf().setAppName(SparkTest.class.getSimpleName())
                .setMaster("local[" + numContainers + "]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            final List<Integer> ranks = Arrays.asList(0, 1);

            sc.parallelize(ranks, numContainers).mapPartitions(iterator -> {
                if (iterator.hasNext()) {
                    final int rank = iterator.next();

                    final String[] args = { "-s", String.valueOf(numContainers), "-r", String.valueOf(rank), "-l",
                            logDirStr };

                    // Execute logic
                    new YarnJobMain(args).run();
                }
                return Collections.singletonList(true).iterator();
            }).collect();
        }
    }
}