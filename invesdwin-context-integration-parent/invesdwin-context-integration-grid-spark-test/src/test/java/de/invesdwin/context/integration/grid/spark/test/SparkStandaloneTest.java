package de.invesdwin.context.integration.grid.spark.test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.filter.DefaultMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.spark.test.job.SparkJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.Files;

@Testcontainers
@NotThreadSafe
public class SparkStandaloneTest extends ATest {

    private static final int NUM_CONTAINERS = 2;

    @Container
    private static final SparkContainer SPARK = new SparkContainer();

    @Test
    public void testSparkOnStandalone() throws Exception {
        final File jobJarFile = new MergedClasspathJar(DefaultMergedClasspathJarFilter.DEFAULT, SparkJobMain.class)
                .getResource()
                .getFile();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final boolean[] jobSuccessful = new boolean[1];

        // 1. Setup local log directory (No HDFS available in Spark Standalone container)
        final File dockerLogDir = new File("/tmp/logs/");

        final Map<String, String> env = ILockCollectionFactory.getInstance(false).newLinkedMap();
        env.putAll(System.getenv());

        final SparkAppHandle handle = new SparkLauncher(env)
                .setSparkHome(SparkContainer.getSparkHomeFolder().getAbsolutePath())

                // 2. Point to the Testcontainers Standalone Master
                .setMaster(SPARK.getMasterUrl())

                // 3. Client mode allows the local driver to access the local JAR file
                .setDeployMode("client")
                .setAppResource(jobJarFile.getAbsolutePath())
                .setMainClass(SparkJobMain.class.getName())

                .setConf("spark.executor.instances", String.valueOf(NUM_CONTAINERS))
                .setConf("spark.executor.cores", "1")
                .setConf("spark.executor.extraJavaOptions", "-Duser.home=/tmp")

                .addAppArgs("--size", String.valueOf(NUM_CONTAINERS), "--logDir",
                        "file://" + dockerLogDir.getAbsolutePath())
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
        Assertions.checkTrue(jobSuccessful[0], "Spark on Standalone job failed!");
        if (!handle.getState().isFinal()) {
            handle.stop();
        }

        // 4. Verify logs directly from the container filesystem
        final File localLogDir = ContextProperties.getCacheDirectory();
        final File log_1_2 = new File(localLogDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(localLogDir, "2_2_LatencyClientTask.log");
        SPARK.copyFileFromContainer(new File(dockerLogDir, log_1_2.getName()).getAbsolutePath(),
                log_1_2.getAbsolutePath());
        SPARK.copyFileFromContainer(new File(dockerLogDir, log_2_2.getName()).getAbsolutePath(),
                log_2_2.getAbsolutePath());
        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}