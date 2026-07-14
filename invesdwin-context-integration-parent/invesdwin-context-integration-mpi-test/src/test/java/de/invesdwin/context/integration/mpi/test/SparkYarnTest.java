package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.hadoop.test.HadoopContainer;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.SparkJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.Files;

@Testcontainers
@NotThreadSafe
public class SparkYarnTest extends ATest {

    private static final int NUM_CONTAINERS = 2;

    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void testSparkOnYarnTrueIsolation() throws Exception {
        final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.HADOOP3, SparkJobMain.class)
                .getResource()
                .getFile();
        final FileSystem fs = FileSystem.get(HADOOP.newHadoopConfiguration());
        final Path hdfsJobJarPath = new Path("/tmp/" + jobJarFile.getName());
        fs.copyFromLocalFile(false, true, new Path(jobJarFile.getAbsolutePath()), hdfsJobJarPath);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final boolean[] jobSuccessful = new boolean[1];

        final String hdfsLogDir = "/tmp/spark-logs"; // Let Spark write logs to HDFS

        final Map<String, String> env = ILockCollectionFactory.getInstance(false).newLinkedMap();
        env.putAll(System.getenv()); // Inherit local variables (like SPARK_HOME)
        env.put("HADOOP_CONF_DIR", HADOOP.getHadoopFolder().getAbsolutePath());

        final SparkAppHandle handle = new SparkLauncher(env)
                // NOTE: You must have Spark installed locally (or in your CI) to use SparkLauncher
                .setSparkHome(System.getenv("SPARK_HOME"))
                .setMaster("yarn")
                .setDeployMode("cluster") // Runs the Driver inside YARN too
                .setAppResource(hdfsJobJarPath.toString())
                .setMainClass(SparkJobMain.class.getName())

                // CRITICAL: Force true container isolation
                .setConf("spark.executor.instances", String.valueOf(NUM_CONTAINERS)) // 2 JVMs!
                .setConf("spark.executor.cores", "1") // 1 Core per JVM ensures tasks split

                .addAppArgs("--size", String.valueOf(NUM_CONTAINERS), "--logDir", hdfsLogDir)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(final SparkAppHandle handle) {
                        if (handle.getState().isFinal()) {
                            jobSuccessful[0] = (handle.getState() == SparkAppHandle.State.FINISHED);
                            countDownLatch.countDown();
                        }
                    }

                    @Override
                    public void infoChanged(final SparkAppHandle handle) {}
                });
        countDownLatch.await();
        Assertions.checkTrue(jobSuccessful[0], "Spark on YARN job failed!");
        handle.stop();

        // 4. Download and verify logs from HDFS
        final File localLogDir = new File(ContextProperties.getCacheDirectory(), "spark-logs");
        fs.copyToLocalFile(new Path(hdfsLogDir), new Path(localLogDir.getAbsolutePath()));

        final File log_1_2 = new File(localLogDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(localLogDir, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}