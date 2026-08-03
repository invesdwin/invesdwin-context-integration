package de.invesdwin.context.integration.grid.ignite3.client.simple;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.ignite3.client.simple.job.SimpleIgnite3ClientJobMain;
import de.invesdwin.context.integration.grid.ignite3.client.simple.job.SimpleIgnite3ClientTask;
import de.invesdwin.context.integration.grid.ignite3.test.Ignite3Container;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
@Testcontainers
public class SimpleIgnite3ClientServerTest extends ATest {

    private static final int NUM_CONTAINERS = 2;

    @Container
    private static final Ignite3Container IGNITE = new Ignite3Container();

    @Test
    public void test() throws Exception {
        final File logDir = ContextProperties.getCacheDirectory();

        final String clientAddress = IGNITE.getClientAddress();
        final String restAddress = IGNITE.getRestAddress();

        final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.DEFAULT, SimpleIgnite3ClientTask.class)
                .getResource()
                .getFile();

        SimpleIgnite3ClientJobMain.main(new String[] { "--size", String.valueOf(NUM_CONTAINERS), "--logDir",
                "file://" + logDir.getAbsolutePath(), "--master", clientAddress, "--rest", restAddress, "--jobJar",
                jobJarFile.getAbsolutePath() });

        final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());

        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}