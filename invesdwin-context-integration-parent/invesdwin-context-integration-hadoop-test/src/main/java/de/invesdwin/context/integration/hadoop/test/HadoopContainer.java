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
public class HadoopContainer extends FixedHostPortGenericContainer<HadoopContainer> {

    private static final int RESOURCEMANAGER_PORT = 8032;
    private static final int DATATRANSFER_PORT = 9866;
    private static final int HDFS_PORT = 9000;
    private static final int RESOURCEMANAGER_HTTP_PORT = 8088;
    private static final int NAMENODE_HTTP_PORT = 9870;
    private static final int DATANODE_HTTP_PORT = 9864;
    private static final int MAPREDUCECLIENT_PORTRANGE_FROM = 49000;
    private static final int MAPREDUCECLIENT_PORTRANGE_TO = 49005;

    private static final Log LOG = new Log(HadoopContainer.class);

    private static final String HADOOP_VERSION = "3.5.0";
    private static final File HADOOP_CONTAINER_FOLDER = new File(ContextProperties.getHomeDataDirectory(),
            HadoopContainer.class.getSimpleName());
    public static final File HADOOP_HOME_FOLDER = new File(HADOOP_CONTAINER_FOLDER, "hadoop");
    //not needed to run the jobs
    private static final boolean HADOOP_FRONTENDS = true;
    //not needed because MpjExpress can work without connection to the host
    private static final boolean HADOOP_EXPOSE_HOST = false;
    private static final boolean FORCE_BUILD_DOCKER_IMAGE = false;

    @SuppressWarnings("deprecation")
    public HadoopContainer() {
        super(newDockerImageName());

        if (HADOOP_FRONTENDS) {
            //dfs.datanode.http.address - The secondary namenode http/https server address and port.
            withFixedExposedPort(DATANODE_HTTP_PORT, DATANODE_HTTP_PORT);
            //dfs.namenode.http-address - The address and the base port where the dfs namenode web ui will listen on.
            withFixedExposedPort(NAMENODE_HTTP_PORT, NAMENODE_HTTP_PORT);
            //yarn.resourcemanager.webapp.address - The http/https address of the RM web application
            withFixedExposedPort(RESOURCEMANAGER_HTTP_PORT, RESOURCEMANAGER_HTTP_PORT);
        }
        //fs.defaultFS - The name of the default file system.
        withFixedExposedPort(HDFS_PORT, HDFS_PORT);
        //DataNode data transfer port (Hadoop 3.x)
        withFixedExposedPort(DATATRANSFER_PORT, DATATRANSFER_PORT);
        //yarn.resourcemanager.address - The address of the applications manager interface in the RM.
        withFixedExposedPort(RESOURCEMANAGER_PORT, 8032);
        setWaitStrategy(new DockerHealthcheckWaitStrategy());
        if (HADOOP_EXPOSE_HOST) {
            withAccessToHost(true);
            //https://stackoverflow.com/a/60740997
            withExtraHost(IntegrationProperties.HOSTNAME, "172.17.0.1");
        }
        for (int i = MAPREDUCECLIENT_PORTRANGE_FROM; i <= MAPREDUCECLIENT_PORTRANGE_TO; i++) {
            withFixedExposedPort(i, i);
        }
    }

    @Override
    public void start() {
        super.start();
        LOG.warn("Hadoop ResourceManager Web UI available at: " + getResourcemanagerHttpUrl());
        LOG.warn("Hadoop NameNode Web UI available at: " + getNamenodeHttpUrl());
        LOG.warn("Hadoop DataNode Web UI available at: " + getDatanodeHttpUrl());
    }

    public String getResourcemanagerHttpUrl() {
        return "http://" + getHost() + ":" + getMappedPort(RESOURCEMANAGER_HTTP_PORT);
    }

    public String getNamenodeHttpUrl() {
        //logs are available at: http://localhost:9870/logs/
        return "http://" + getHost() + ":" + getMappedPort(NAMENODE_HTTP_PORT);
    }

    public String getDatanodeHttpUrl() {
        return "http://" + getHost() + ":" + getMappedPort(DATANODE_HTTP_PORT);
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
                .withFileFromPath(".", HADOOP_CONTAINER_FOLDER.toPath())
                .get();

        // Note: ImageFromDockerfile automatically tags it with the name we provided
        return generatedImageName;
    }

    public static String getHadoopVersion() {
        return HADOOP_VERSION;
    }

    public static File getHadoopHomeFolder() {
        maybeDownloadAndExtractHadoop();
        return HADOOP_HOME_FOLDER;
    }

    private static void maybeDownloadAndExtractHadoop() {
        try {
            final Resource[] resources = PreMergedContext.getInstance()
                    .getResources(
                            "classpath*:/" + HadoopContainer.class.getPackageName().replace(".", "/") + "/files/*");
            for (int i = 0; i < resources.length; i++) {
                final Resource resource = resources[i];
                final File file = new File(HADOOP_CONTAINER_FOLDER, resource.getFilename());
                Files.copyInputStreamToFile(resource.getInputStream(), file);
                if (file.getName().equals("docker-entrypoint.sh")) {
                    file.setExecutable(true, false);
                }
            }

            final File hadoopVersionedFolder = new File(HADOOP_HOME_FOLDER.getAbsolutePath() + "-" + HADOOP_VERSION);
            final File hadoopFile = new File(HADOOP_CONTAINER_FOLDER, "hadoop-" + HADOOP_VERSION + ".tar.gz");
            if (!hadoopFile.exists()) {
                final Instant started = new Instant();
                LOG.info("Started downloading [%s]", hadoopFile);
                final File hadoopFilePart = new File(hadoopFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(hadoopFilePart);
                IOUtils.copy(URIs.asUrl("https://archive.apache.org/dist/hadoop/common/hadoop-" + HADOOP_VERSION
                        + "/hadoop-" + HADOOP_VERSION + ".tar.gz"), hadoopFilePart);
                Files.moveFileQuietly(hadoopFilePart, hadoopFile);
                Files.deleteQuietly(HADOOP_HOME_FOLDER);
                Files.deleteQuietly(hadoopVersionedFolder);
                LOG.info("Finished downloading [%s] after %s", hadoopFile, started);
            }
            if (!HADOOP_HOME_FOLDER.exists()) {
                final Instant started = new Instant();
                LOG.info("Started extracting [%s]", hadoopFile);
                final Archiver archiver = ArchiverFactory.createArchiver(hadoopFile);
                archiver.extract(hadoopFile, hadoopVersionedFolder.getParentFile());
                Assertions.assertThat(hadoopVersionedFolder).exists();
                Files.moveDirectory(hadoopVersionedFolder, HADOOP_HOME_FOLDER);
                //                COPY core-site.xml $HADOOP_HOME/etc/hadoop/
                for (final String filename : new String[] { "core-site.xml", "hdfs-site.xml", "yarn-site.xml" }) {
                    Files.copyFile(new File(HADOOP_CONTAINER_FOLDER, filename),
                            new File(HADOOP_HOME_FOLDER, "etc/hadoop/" + filename));
                }
                LOG.info("Finished extracting [%s] after %s", hadoopFile, started);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Configuration newHadoopConfiguration() {
        final Configuration conf = new Configuration();
        putProperties(conf);
        return conf;
    }

    public YarnConfiguration newYarnConfiguration() {
        //CHECKSTYLE:OFF
        System.setProperty("hadoop.home.dir", getHadoopHomeFolder().getAbsolutePath());
        //CHECKSTYLE:ON
        final YarnConfiguration conf = new YarnConfiguration();
        putProperties(conf);
        return conf;
    }

    private void putProperties(final Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://localhost:" + HDFS_PORT);
        conf.set("yarn.resourcemanager.address", "localhost:" + RESOURCEMANAGER_PORT);
        conf.set("yarn.nodemanager.hostname", "localhost");
        conf.set("yarn.nodemanager.address", "localhost:8041");
        conf.set("yarn.nodemanager.webapp.address", "localhost:8042");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.app.mapreduce.am.job.client.port-range",
                MAPREDUCECLIENT_PORTRANGE_FROM + "-" + MAPREDUCECLIENT_PORTRANGE_TO);
        conf.set("yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
        conf.set("mapreduce.map.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
        conf.set("mapreduce.reduce.env", "HADOOP_MAPRED_HOME=/home/hduser/hadoop");
    }

}
