package de.invesdwin.context.integration.hadoop.test;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.springframework.core.io.Resource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.Instant;

@NotThreadSafe
public class SparkContainer extends FixedHostPortGenericContainer<SparkContainer> {

    private static final Log LOG = new Log(SparkContainer.class);

    private static final String HADOOP_VERSION = "3.5.0";
    private static final File HADOOP_DOCKER_FOLDER = new File(ContextProperties.getHomeDataDirectory(),
            SparkContainer.class.getSimpleName());
    public static final File HADOOP_FOLDER = new File(HADOOP_DOCKER_FOLDER, "hadoop");
    //not needed to run the jobs
    private static final boolean HADOOP_FRONTENDS = true;
    //not needed because MpjExpress can work without connection to the host
    private static final boolean HADOOP_EXPOSE_HOST = false;
    private static final boolean FORCE_BUILD_DOCKER_IMAGE = false;

    @SuppressWarnings("deprecation")
    public SparkContainer() {
        super(newDockerImageName());

        if (HADOOP_FRONTENDS) {
            //dfs.datanode.http.address - The secondary namenode http/https server address and port.
            withFixedExposedPort(9864, 9864);
            //dfs.namenode.http-address - The address and the base port where the dfs namenode web ui will listen on.
            withFixedExposedPort(9870, 9870);
            //yarn.resourcemanager.webapp.address - The http/https address of the RM web application
            withFixedExposedPort(8088, 8088);
        }
        //fs.defaultFS - The name of the default file system.
        withFixedExposedPort(9000, 9000);
        //DataNode data transfer port (Hadoop 3.x)
        withFixedExposedPort(9866, 9866);
        //yarn.resourcemanager.address - The address of the applications manager interface in the RM.
        withFixedExposedPort(8032, 8032);
        setWaitStrategy(new DockerHealthcheckWaitStrategy());
        if (HADOOP_EXPOSE_HOST) {
            withAccessToHost(true);
            //https://stackoverflow.com/a/60740997
            withExtraHost(IntegrationProperties.HOSTNAME, "172.17.0.1");
        }
        for (int i = 49000; i <= 49005; i++) {
            withFixedExposedPort(i, i);
        }

        //logs are available at: http://localhost:9870/logs/
    }

    private static String newDockerImageName() {
        if (HADOOP_EXPOSE_HOST) {
            org.testcontainers.Testcontainers.exposeHostPorts(40002);
        }

        maybeDownloadAndExtractHadoop();

        final String targetImageName = "invesdwin/hadoop-single-node:" + HADOOP_VERSION;
        if (FORCE_BUILD_DOCKER_IMAGE) {
            LOG.info("Forcing build of Docker image [%s]...", targetImageName);
            return buildDockerImage(targetImageName);
        } else {
            final DockerClient dockerClient = DockerClientFactory.lazyClient();
            try {
                // 1. Check if the image already exists in the local Docker daemon
                dockerClient.inspectImageCmd(targetImageName).exec();
                LOG.info("Found cached Docker image: [%s]. Skipping build.", targetImageName);
                return targetImageName;
            } catch (final NotFoundException e) {
                // 2. If it doesn't exist, build it
                LOG.info("Docker image [%s] not found. Building it now...", targetImageName);
                return buildDockerImage(targetImageName);
            }
        }
    }

    private static String buildDockerImage(final String targetImageName) {
        final String generatedImageName = new ImageFromDockerfile(targetImageName, false)
                .withFileFromPath(".", HADOOP_DOCKER_FOLDER.toPath())
                .get();

        // Note: ImageFromDockerfile automatically tags it with the name we provided
        return generatedImageName;
    }

    private static void maybeDownloadAndExtractHadoop() {
        try {
            final Resource[] resources = PreMergedContext.getInstance()
                    .getResources(
                            "classpath*:/" + SparkContainer.class.getPackageName().replace(".", "/") + "/files/*");
            for (int i = 0; i < resources.length; i++) {
                final Resource resource = resources[i];
                final File file = new File(HADOOP_DOCKER_FOLDER, resource.getFilename());
                Files.copyInputStreamToFile(resource.getInputStream(), file);
                if (file.getName().equals("docker-entrypoint.sh")) {
                    file.setExecutable(true, false);
                }
            }

            final File hadoopVersionedFolder = new File(HADOOP_FOLDER.getAbsolutePath() + "-" + HADOOP_VERSION);
            final File hadoopFile = new File(HADOOP_DOCKER_FOLDER, "hadoop-" + HADOOP_VERSION + ".tar.gz");
            if (!hadoopFile.exists()) {
                final Instant started = new Instant();
                LOG.info("Started downloading [%s]", hadoopFile);
                final File hadoopFilePart = new File(hadoopFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(hadoopFilePart);
                IOUtils.copy(URIs.asUrl("https://archive.apache.org/dist/hadoop/common/hadoop-" + HADOOP_VERSION
                        + "/hadoop-" + HADOOP_VERSION + ".tar.gz"), hadoopFilePart);
                Files.moveFileQuietly(hadoopFilePart, hadoopFile);
                Files.deleteQuietly(HADOOP_FOLDER);
                Files.deleteQuietly(hadoopVersionedFolder);
                LOG.info("Finished downloading [%s] after %s", hadoopFile, started);
            }
            if (!HADOOP_FOLDER.exists()) {
                final Instant started = new Instant();
                LOG.info("Started extracting [%s]", hadoopFile);
                final Archiver archiver = ArchiverFactory.createArchiver(hadoopFile);
                archiver.extract(hadoopFile, hadoopVersionedFolder.getParentFile());
                Assertions.assertThat(hadoopVersionedFolder).exists();
                Files.moveDirectory(hadoopVersionedFolder, HADOOP_FOLDER);
                //                COPY core-site.xml $HADOOP_HOME/etc/hadoop/
                for (final String filename : new String[] { "core-site.xml", "hdfs-site.xml", "yarn-site.xml" }) {
                    Files.copyFile(new File(HADOOP_DOCKER_FOLDER, filename),
                            new File(HADOOP_FOLDER, "etc/hadoop/" + filename));
                }
                LOG.info("Finished extracting [%s] after %s", hadoopFile, started);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public File getHadoopFolder() {
        return HADOOP_FOLDER;
    }

    public String getHadoopVersion() {
        return HADOOP_VERSION;
    }

    public Configuration newHadoopConfiguration() {
        final Configuration conf = new Configuration();
        putProperties(conf);
        return conf;
    }

    public YarnConfiguration newYarnConfiguration() {
        //CHECKSTYLE:OFF
        System.setProperty("hadoop.home.dir", getHadoopFolder().getAbsolutePath());
        //CHECKSTYLE:ON
        final YarnConfiguration conf = new YarnConfiguration();
        putProperties(conf);
        return conf;
    }

    private void putProperties(final Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("yarn.nodemanager.hostname", "localhost");
        conf.set("yarn.nodemanager.address", "localhost:8041");
        conf.set("yarn.nodemanager.webapp.address", "localhost:8042");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.app.mapreduce.am.job.client.port-range", "49000-49005");
        conf.set("yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
        conf.set("mapreduce.map.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
        conf.set("mapreduce.reduce.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
    }

}
