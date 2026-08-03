package de.invesdwin.context.integration.grid.ignite3.node.simple;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.StartedProcess;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.ignite3.node.simple.job.SimpleIgnite3NodeLocalJobMain;
import de.invesdwin.context.integration.grid.ignite3.node.simple.job.SimpleIgnite3NodeTask;
import de.invesdwin.context.integration.grid.jar.ForkJobHelper;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.PackageMergedClasspathJarFilter;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class SimpleIgnite3NodeServerTest extends ATest {

    private static final int NUM_NODES = 2;

    @Test
    public void test() throws Exception {
        final File logDir = ContextProperties.getCacheDirectory();

        final File jobJarFile = new MergedClasspathJar(
                new PackageMergedClasspathJarFilter(SimpleIgnite3NodeTask.class.getPackageName()),
                SimpleIgnite3NodeTask.class).getResource().getFile();

        // 1. Fork the second node (worker node) asynchronously via ForkJobHelper
        final StartedProcess workerProcess = ForkJobHelper.forkAsync(SimpleIgnite3NodeLocalJobMain.class,
                new String[] { "--nodeName", "worker-node", "--port", "3345" });

        try {
            // Give the worker process a moment to initialize and listen
            Thread.sleep(3000);

            // 2. Run the master node (node-0 on port 3344) in the test JVM
            SimpleIgnite3NodeLocalJobMain.main(new String[] { "--nodeName", "master-node", "--port", "3344", "--size",
                    String.valueOf(NUM_NODES), "--logDir", logDir.getAbsolutePath(), "--master", "--jobJar",
                    jobJarFile.getAbsolutePath() });

            final File log_1_2 = new File(logDir, "1_2_LatencyServerTask.log");
            final File log_2_2 = new File(logDir, "2_2_LatencyClientTask.log");

            final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
            final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());

            Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
            Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");

        } finally {
            // 3. Ensure worker process is terminated cleanly
            if (workerProcess.getProcess().isAlive()) {
                workerProcess.getProcess().destroyForcibly();
            }
        }
    }
}