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
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.MpiJobMainJar;
import de.invesdwin.context.integration.mpi.test.job.MpiJobYarnMain;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;

@NotThreadSafe
@Testcontainers
public class MpjExpressYarnTest extends ATest {

    //not needed to run the jobs
    private static final boolean ACCESS_HADOOP_FRONTENDS = true;
    //not needed because MpjExpress can work without connection to the host
    private static final boolean EXPOSE_HOST = false;
    private static final String HADOOP_VERSION = "3.3.4";
    private static final File HADOOP_DOCKER_FOLDER = new File("mpj/hadoop/");
    private static final File HADOOP_FOLDER = new File(HADOOP_DOCKER_FOLDER, "hadoop");
    @Container
    private static final GenericContainer<?> HADOOP = newHadoopContainer();

    @SuppressWarnings({ "deprecation", "resource" })
    private static GenericContainer<?> newHadoopContainer() {
        if (EXPOSE_HOST) {
            org.testcontainers.Testcontainers.exposeHostPorts(40002);
        }
        maybeDownloadAndExtractHadoop();
        final FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(
                new ImageFromDockerfile(MpjExpressYarnTest.class.getSimpleName().toLowerCase())
                        .withFileFromPath(".", HADOOP_DOCKER_FOLDER.toPath())
                        .get());
        if (ACCESS_HADOOP_FRONTENDS) {
            //dfs.datanode.http.address - The secondary namenode http/https server address and port.
            container.withFixedExposedPort(9864, 9864);
            //dfs.namenode.http-address - The address and the base port where the dfs namenode web ui will listen on.
            container.withFixedExposedPort(9870, 9870);
            //yarn.resourcemanager.webapp.address - The http/https address of the RM web application
            container.withFixedExposedPort(8088, 8088);
        }
        //fs.defaultFS - The name of the default file system.
        container.withFixedExposedPort(9000, 9000);
        //yarn.resourcemanager.address - The address of the applications manager interface in the RM.
        container.withFixedExposedPort(8032, 8032);
        container.setWaitStrategy(new DockerHealthcheckWaitStrategy());
        if (EXPOSE_HOST) {
            container.withAccessToHost(true);
            //https://stackoverflow.com/a/60740997
            container.withExtraHost(IntegrationProperties.HOSTNAME, "172.17.0.1");
        }

        //logs are available at: http://localhost:9870/logs/
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
                //                COPY core-site.xml $HADOOP_HOME/etc/hadoop/
                for (final String filename : new String[] { "core-site.xml", "hdfs-site.xml", "yarn-site.xml" }) {
                    Files.copyFile(new File(HADOOP_DOCKER_FOLDER, filename),
                            new File(HADOOP_FOLDER, "etc/hadoop/" + filename));
                }
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
        final StringBuilder args = new StringBuilder();
        args.append("-yarn -np 2 -dev niodev -hdfsFolder \"/tmp/" + getClass().getSimpleName() + "\" ");
        //        args.append("-debugYarn");
        args.append(" -wdir \"");
        args.append(workDir.getAbsolutePath());
        args.append("\" -jar ");
        args.append(new MpiJobMainJar(MergedClasspathJarFilter.MPI_YARN3).getResource().getFile().getAbsolutePath());
        args.append(" ");
        args.append(MpiJobYarnMain.class.getName());
        script = script.replace("{ARGS}", args.toString());
        final File scriptFile = new File(ContextProperties.getCacheDirectory(), "mpjexpressyarn_test.sh");
        Files.writeStringToFile(scriptFile, script, Charset.defaultCharset());

        new ProcessExecutor().command("sh", scriptFile.getAbsolutePath())
                .destroyOnExit()
                .exitValueNormal()
                .redirectOutput(Slf4jStream.of(getClass()).asInfo())
                .redirectError(Slf4jStream.of(getClass()).asWarn())
                .execute();
    }

}
