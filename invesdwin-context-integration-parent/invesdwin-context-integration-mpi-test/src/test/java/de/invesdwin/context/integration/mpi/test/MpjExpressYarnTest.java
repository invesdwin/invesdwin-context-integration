package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.zeroturnaround.exec.ProcessExecutor;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.hadoop.docker.HadoopContainer;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMain;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.log.LogLevelOutputStream;

@NotThreadSafe
@Testcontainers
public class MpjExpressYarnTest extends AMpiTest {

    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void test() throws Throwable {
        final File workDir = new File(ContextProperties.getCacheDirectory(), "work");
        Files.forceMkdir(workDir);

        final File scriptTemplate = new File("mpj/mpjexpressyarn_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{MPJ_HOME}", new File("mpj/MpjExpress-v0_44").getAbsolutePath());
        script = script.replace("{JAVA_HOME}", new SystemProperties().getString("java.home"));
        script = script.replace("{HADOOP_HOME}", HADOOP.getHadoopFolder().getAbsolutePath());
        final StringBuilder args = new StringBuilder();
        args.append("-yarn -np 2 -dev niodev -hdfsFolder \"/tmp/" + getClass().getSimpleName() + "\" ");
        //args.append("-debugYarn");
        args.append(" -wdir \"");
        args.append(workDir.getAbsolutePath());
        args.append("\" -jar ");
        args.append(new MergedClasspathJar(MergedClasspathJarFilter.MPI_YARN3, MpiJobMain.class).getResource()
                .getFile()
                .getAbsolutePath());
        args.append(" ");
        args.append(MpiJobMain.class.getName());
        script = script.replace("{ARGS}", args.toString());
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "mpjexpressyarn_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(new LogLevelOutputStream(LogLevel.INFO, log))
                .redirectError(new LogLevelOutputStream(LogLevel.WARN, log))
                .execute();
    }

}
