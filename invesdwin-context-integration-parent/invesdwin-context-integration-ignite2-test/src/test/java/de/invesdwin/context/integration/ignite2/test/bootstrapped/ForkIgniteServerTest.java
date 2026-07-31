package de.invesdwin.context.integration.ignite2.test.bootstrapped;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.ignite2.test.Ignite2Container;
import de.invesdwin.context.integration.ignite2.test.bootstrapped.job.ForkIgniteJobMain;
import de.invesdwin.context.integration.ignite2.test.bootstrapped.job.ForkIgniteTaskMain;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

/**
 * Example on how to jail break the ignite classloader in order to perform an invesdwin bootstrap in an unrestricted
 * JVM.
 */
@Testcontainers
@NotThreadSafe
public class ForkIgniteServerTest extends ATest {

    private static final int NUM_CONTAINERS = 2;

    @Container
    private static final Ignite2Container IGNITE = new Ignite2Container();

    @Test
    public void test() throws Exception {
        final File logDir = ContextProperties.getCacheDirectory();
        final String masterAddress = IGNITE.getDiscoveryAddress();

        // Create the job JAR on the fly from the outside
        final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.DEFAULT, ForkIgniteTaskMain.class)
                .getResource()
                .getFile();

        // Copy the JAR into the Testcontainers Docker container so remote worker nodes can access it via path
        final String containerJarPath = "/tmp/" + jobJarFile.getName();
        IGNITE.copyFileToContainer(MountableFile.forHostPath(jobJarFile.getAbsolutePath()), containerJarPath);

        // Pass the job JAR path to ForkIgniteJobMain
        ForkIgniteJobMain.main(new String[] { "--size", String.valueOf(NUM_CONTAINERS), "--logDir",
                "file://" + logDir.getAbsolutePath(), "--master", masterAddress, "--jobJar", containerJarPath });

        final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());

        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}