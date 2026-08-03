package de.invesdwin.context.integration.grid.ignite2.test.simple;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.ignite2.test.Ignite2Container;
import de.invesdwin.context.integration.grid.ignite2.test.simple.job.SimpleIgnite2JobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@Testcontainers
@NotThreadSafe
public class SimpleIgnite2ServerTest extends ATest {

    private static final int NUM_CONTAINERS = 2;

    @Container
    private static final Ignite2Container IGNITE = new Ignite2Container();

    @Test
    public void test() throws Exception {
        final File logDir = ContextProperties.getCacheDirectory();

        // Retrieve mapped host and port from Testcontainers
        final String masterAddress = IGNITE.getDiscoveryAddress();

        // Pass --master address to configure the job as a client connecting to Docker
        SimpleIgnite2JobMain.main(new String[] { "--size", String.valueOf(NUM_CONTAINERS), "--logDir",
                "file://" + logDir.getAbsolutePath(), "--master", masterAddress });

        final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());

        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}