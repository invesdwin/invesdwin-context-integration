package de.invesdwin.context.integration.grid.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.DefaultMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.mpi.test.job.MpiJobMain;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.log.LogLevelOutputStream;

@NotThreadSafe
public class SlurmSbatchOpenMpiTest extends AMpiTest {

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/slurm_sbatch_openmpi_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{WORKDIR}", ContextProperties.getCacheDirectory().getAbsolutePath());
        script = script.replace("{ARGS}",
                " java -jar "
                        + new MergedClasspathJar(DefaultMergedClasspathJarFilter.DEFAULT, MpiJobMain.class).getResource()
                                .getFile()
                                .getAbsolutePath()
                        + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "slurm_sbatch_openmpi_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor().command("sbatch", "--wait", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(new LogLevelOutputStream(LogLevel.INFO, log))
                .redirectError(new LogLevelOutputStream(LogLevel.WARN, log))
                .execute();
    }

}
