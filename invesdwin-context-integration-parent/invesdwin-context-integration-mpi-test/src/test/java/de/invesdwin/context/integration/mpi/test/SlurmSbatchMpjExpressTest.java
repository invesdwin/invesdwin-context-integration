package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class SlurmSbatchMpjExpressTest extends AMpiTest {

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/slurm_sbatch_mpjexpress_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());

        script = script.replace("{WORKDIR}", ContextProperties.getCacheDirectory().getAbsolutePath());
        script = script.replace("{MPJ_HOME}", new File("mpj/MpjExpress-v0_44").getAbsolutePath());
        script = script.replace("{JAVA_HOME}", new SystemProperties().getString("java.home"));
        script = script.replace("{ARGS}",
                " -jar " + new MpiJobMainJar(MergedClasspathJarFilter.MPI).getResource().getFile().getAbsolutePath()
                        + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");

        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "slurm_sbatch_mpjexpress_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor().command("sbatch", "--wait", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(Slf4jStream.of(getClass()).asInfo())
                .redirectError(Slf4jStream.of(getClass()).asWarn())
                .execute();
    }

}
