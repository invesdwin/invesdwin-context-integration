package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMain;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.log.LogLevelOutputStream;

@NotThreadSafe
public class SlurmSbatchMpjExpressTest extends AMpiTest {

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/slurm_sbatch_mpjexpress_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());

        script = script.replace("{WORKDIR}", ContextProperties.getCacheDirectory().getAbsolutePath());
        script = script.replace("{MPJ_HOME}", new File("mpj/MpjExpress-v0_44_kevinmilner").getAbsolutePath());
        script = script.replace("{JAVA_HOME}", new SystemProperties().getString("java.home"));
        script = script.replace("{ARGS}",
                " -jar " + new MergedClasspathJar(MergedClasspathJarFilter.MPI, MpiJobMain.class).getResource()
                        .getFile()
                        .getAbsolutePath() + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath()
                        + "\"");

        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "slurm_sbatch_mpjexpress_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        //WARNING: does not execute the jobs on different nodes, instead on the same node, OpenMPI work on multiple nodes
        new ProcessExecutor().command("sbatch", "--wait", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(new LogLevelOutputStream(LogLevel.INFO, log))
                .redirectError(new LogLevelOutputStream(LogLevel.WARN, log))
                .execute();
    }

}
