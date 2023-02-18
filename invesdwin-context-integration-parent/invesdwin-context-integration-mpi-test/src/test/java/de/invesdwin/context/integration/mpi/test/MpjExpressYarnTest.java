package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;

@NotThreadSafe
@Testcontainers
public class MpjExpressYarnTest extends ATest {

    private static final String HADOOP_VERSION = "3.3.4";
    private static final File HADOOP_FOLDER = new File("mpj/hadoop/hadoop");
    @Container
    private static final GenericContainer<?> HADOOP = newHadoopContainer();

    @SuppressWarnings({ "deprecation", "resource" })
    private static GenericContainer<?> newHadoopContainer() {
        maybeDownloadAndExtractHadoop();
        final FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(
                new ImageFromDockerfile(MpjExpressYarnTest.class.getSimpleName().toLowerCase())
                        .withFileFromPath(".", new File("mpj/hadoop/").toPath())
                        .get())
                                //dfs.datanode.http.address - The secondary namenode http/https server address and port.
                                .withFixedExposedPort(9864, 9864)
                                //dfs.namenode.http-address - The address and the base port where the dfs namenode web ui will listen on.
                                .withFixedExposedPort(9870, 9870)
                                //yarn.resourcemanager.webapp.address - The http/https address of the RM web application
                                .withFixedExposedPort(8088, 8088)
                                //fs.defaultFS - The name of the default file system.
                                .withFixedExposedPort(9000, 9000)
                                //yarn.resourcemanager.address - The address of the applications manager interface in the RM.
                                .withFixedExposedPort(8032, 8032);
        container.setWaitStrategy(new DockerHealthcheckWaitStrategy());
        return container;
    }

    private static void maybeDownloadAndExtractHadoop() {
        final File hadoopVersionedFolder = new File(HADOOP_FOLDER.getAbsolutePath() + "-" + HADOOP_VERSION);
        final File hadoopFile = new File("mpj/hadoop/hadoop-" + HADOOP_VERSION + ".tar.gz");
        try {
            if (!hadoopFile.exists()) {
                final File hadoopFilePart = new File(hadoopFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(hadoopFilePart);
                IOUtils.copy(URIs.asUrl("http://archive.apache.org/dist/hadoop/common/hadoop-" + HADOOP_VERSION
                        + "/hadoop-" + HADOOP_VERSION + ".tar.gz"), hadoopFilePart);
                hadoopFilePart.renameTo(hadoopFile);
                Files.deleteQuietly(HADOOP_FOLDER);
                Files.deleteQuietly(hadoopVersionedFolder);
            }
            if (!HADOOP_FOLDER.exists()) {
                final Archiver archiver = ArchiverFactory.createArchiver(hadoopFile);
                archiver.extract(hadoopFile, hadoopVersionedFolder.getParentFile());
                Assertions.assertThat(hadoopVersionedFolder).exists();
                hadoopVersionedFolder.renameTo(HADOOP_FOLDER);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() throws Throwable {
        final File workDir = new File(ContextProperties.getCacheDirectory(), "work");
        Files.forceMkdir(workDir);

        final File scriptTemplate = new File("mpj/mpjexpressyarn_test_template.sh");
        String script = Files.readFileToString(scriptTemplate, Charset.defaultCharset());
        script = script.replace("{MPJ_HOME}", new File("mpj/MpjExpress-v0_44").getAbsolutePath());
        script = script.replace("{JAVA_HOME}", new SystemProperties().getString("java.home"));
        script = script.replace("{HADOOP_HOME}", HADOOP_FOLDER.getAbsolutePath());
        script = script.replace("{ARGS}",
                "-yarn -np 2 -dev niodev -wdir \"" + workDir.getAbsolutePath() + "\" -jar "
                        + new MpiJobMainJar(MergedClasspathJarFilter.MPI_YARN).getResource().getFile().getAbsolutePath()
                        + " --logDir \"" + ContextProperties.getCacheDirectory().getAbsolutePath() + "\"");
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "mpjexpressyarn_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        final ProcessResult result = new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .redirectOutput(Slf4jStream.of(getClass()).asInfo())
                .redirectError(Slf4jStream.of(getClass()).asWarn())
                .execute();
        Assertions.checkEquals(0, result.getExitValue());
    }

}
