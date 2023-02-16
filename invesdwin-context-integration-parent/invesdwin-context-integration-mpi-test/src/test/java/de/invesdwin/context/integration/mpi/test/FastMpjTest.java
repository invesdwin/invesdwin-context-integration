package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class FastMpjTest extends ATest {

    public FastMpjTest() {}

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/fastmpj_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{FMPJ_HOME}", new File("mpj/FastMpj-1.0_7").getAbsolutePath());
        script = script.replace("{JAVA_HOME}", new SystemProperties().getString("java.home"));
        script = script.replace("{ARGS}",
                "-np 2 -jar " + MpiJobMainJar.INSTANCE.getResource().getFile().getAbsolutePath() + " --logDir \""
                        + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "fastmpj_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        final ProcessResult result = new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .redirectOutput(Slf4jStream.of(FastMpjTest.class).asInfo())
                .redirectError(Slf4jStream.of(FastMpjTest.class).asWarn())
                .execute();
        Assertions.checkEquals(0, result.getExitValue());
    }

}
