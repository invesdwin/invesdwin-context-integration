package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class OpenMpiTest extends ATest {

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/openmpi_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{ARGS}",
                "-np 2 java -jar "
                        + new MpiJobMainJar(MergedClasspathJarFilter.DEFAULT).getResource().getFile().getAbsolutePath()
                        + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "openmpi_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        final ProcessResult result = new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .redirectOutput(Slf4jStream.of(getClass()).asInfo())
                .redirectError(Slf4jStream.of(getClass()).asWarn())
                .execute();
        Assertions.checkEquals(0, result.getExitValue());
    }

}
