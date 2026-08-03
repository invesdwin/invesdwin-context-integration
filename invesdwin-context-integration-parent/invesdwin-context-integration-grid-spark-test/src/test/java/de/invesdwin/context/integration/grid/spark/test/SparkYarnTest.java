package de.invesdwin.context.integration.grid.spark.test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.MutableBoolean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.hadoop.test.HadoopContainer;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.filter.DefaultMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.spark.test.job.SparkJobMain;
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
    public void testSparkOnYarn() throws Exception {
        final File jobJarFile = new MergedClasspathJar(DefaultMergedClasspathJarFilter.DEFAULT, SparkJobMain.class)
                .getResource()
                .getFile();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MutableBoolean jobSuccessful = new MutableBoolean();

        final String hdfsLogDir = "/tmp/logs/";
        final FileSystem fs = FileSystem.get(HADOOP.newHadoopConfiguration());
        final String defaultFs = fs.getUri().toString();
        final Map<String, String> env = ILockCollectionFactory.getInstance(false).newLinkedMap();
        env.putAll(System.getenv());
        env.put("SPARK_HOME", SparkContainer.getSparkHomeFolder().getAbsolutePath());
        env.put("HADOOP_CONF_DIR", HadoopContainer.getHadoopHomeFolder().getAbsolutePath());

        final SparkAppHandle handle = new SparkLauncher(env)
                .setSparkHome(SparkContainer.getSparkHomeFolder().getAbsolutePath())
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setAppResource(jobJarFile.getAbsolutePath())
                .setMainClass(SparkJobMain.class.getName())

                .setConf("spark.hadoop.fs.defaultFS", defaultFs)
                .setConf("spark.yarn.stagingDir", defaultFs + "/tmp/spark-staging")

                .setConf("spark.executor.instances", String.valueOf(NUM_CONTAINERS))
                .setConf("spark.executor.cores", "1")

                .addAppArgs("--size", String.valueOf(NUM_CONTAINERS), "--logDir", defaultFs + hdfsLogDir)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(final SparkAppHandle handle) {
                        if (handle.getState().isFinal()) {
                            jobSuccessful.set(handle.getState() == SparkAppHandle.State.FINISHED);
                            countDownLatch.countDown();
                        }
                    }

                    @Override
                    public void infoChanged(final SparkAppHandle handle) {}
                });
        countDownLatch.await();
        Assertions.checkTrue(jobSuccessful.get(), "Spark on YARN job failed!");
        if (!handle.getState().isFinal()) {
            handle.stop();
        }

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