package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextDirectoriesStub;
import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class SlurmSrunOpenMpiTest extends ATest {

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(ContextDirectoriesStub.class);
        Files.deleteNative(ContextProperties.getCacheDirectory());
        Files.forceMkdir(ContextProperties.getCacheDirectory());
    }

    @Test
    public void test() throws Throwable {
        final File scriptTemplate = new File("mpj/slurm_srun_openmpi_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{WORKDIR}", ContextProperties.getCacheDirectory().getAbsolutePath());
        script = script.replace("{ARGS}",
                " java -jar "
                        + new MpiJobMainJar(MergedClasspathJarFilter.DEFAULT).getResource().getFile().getAbsolutePath()
                        + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "slurm_srun_openmpi_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor()
                .command("srun", "--nodes=2", "--ntasks-per-node=1", "--mem=2g", "--time=00:00:10", "--partition=nodes",
                        "bash", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(Slf4jStream.of(getClass()).asInfo())
                .redirectError(Slf4jStream.of(getClass()).asWarn())
                .execute();
    }

}
