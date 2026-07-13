package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.integration.mpi.test.job.NoMpiJobMain;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.log.LogLevelOutputStream;

@NotThreadSafe
public class NoOpenMpiTest extends AMpiTest {

    @Test
    public void test() throws Throwable {
        /**
         * we use a script to do the environment variable conversion, alternatively the job itself could read the env
         * variables, but those are different depending on the MPI or scheduler implementation used.
         */
        final File jobTemplate = new File("mpj/job_test_template.sh");
        String job = Files.readFileToString(jobTemplate, Charset.defaultCharset());
        job = job.replace("{ARGS}", "java -jar "
                + new MpiJobMainJar(MergedClasspathJarFilter.MPI, NoMpiJobMain.class).getResource()
                        .getFile()
                        .getAbsolutePath()
                + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\""
                + " --size $OMPI_COMM_WORLD_SIZE --rank $OMPI_COMM_WORLD_RANK");
        final File jobFile = new File(ContextProperties.getCacheDirectory(), "job_test.sh");
        Files.writeStringToFile(jobFile, job, Charset.defaultCharset());

        final File scriptTemplate = new File("mpj/openmpi_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{ARGS}", "-np 2 sh " + jobFile.getAbsolutePath());
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "openmpi_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(new LogLevelOutputStream(LogLevel.INFO, log))
                .redirectError(new LogLevelOutputStream(LogLevel.WARN, log))
                .execute();
    }

}
