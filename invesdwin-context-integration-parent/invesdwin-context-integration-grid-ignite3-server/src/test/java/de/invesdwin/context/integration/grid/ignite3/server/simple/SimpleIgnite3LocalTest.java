package de.invesdwin.context.integration.grid.ignite3.server.simple;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.ignite3.server.simple.job.SimpleIgnite3ServerJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class SimpleIgnite3LocalTest extends ATest {

    private static final int NUM_CONTAINERS = 2; // Representing compute job workers

    @Test
    public void test() throws Exception {
        final File logDir = ContextProperties.getCacheDirectory();

        SimpleIgnite3ServerJobMain.main(new String[] { "--nodeName", "local-node", "--port", "3344", "--size",
                String.valueOf(NUM_CONTAINERS), "--logDir", logDir.getAbsolutePath(), "--master" });

        final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());

        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}