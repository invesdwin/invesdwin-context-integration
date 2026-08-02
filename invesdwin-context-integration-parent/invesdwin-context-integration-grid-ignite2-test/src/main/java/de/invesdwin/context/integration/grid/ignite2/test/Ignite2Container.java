package de.invesdwin.context.integration.grid.ignite2.test;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.springframework.core.io.Resource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.Instant;

@NotThreadSafe
public class Ignite2Container extends GenericContainer<Ignite2Container> {

    public static final String IGNITE_VERSION = "2.18.0";

    public static final int DISCOVERY_PORT = 47500;
    public static final int COMMUNICATION_PORT = 47100;

    private static final Log LOG = new Log(Ignite2Container.class);

    private static final File IGNITE_CONTAINER_FOLDER = new File(ContextProperties.getHomeDataDirectory(),
            Ignite2Container.class.getSimpleName());
    private static final File IGNITE_HOME_FOLDER = new File(IGNITE_CONTAINER_FOLDER, "ignite");
    private static final boolean FORCE_BUILD_DOCKER_IMAGE = false;

    public Ignite2Container() {
        this(DockerImageName.parse(newDockerImageName()));
    }

    public Ignite2Container(final DockerImageName image) {
        super(image);

        // Expose Ignite Discovery and Communication Ports
        addFixedExposedPort(DISCOVERY_PORT, DISCOVERY_PORT);
        addFixedExposedPort(COMMUNICATION_PORT, COMMUNICATION_PORT);

        // Wait for the node to complete topology snapshot setup
        waitingFor(Wait.forLogMessage(".*Topology snapshot.*", 1));
    }

    @Override
    public void start() {
        super.start();
        LOG.info("Ignite container started at [%s]", getDiscoveryAddress());
    }

    public String getDiscoveryAddress() {
        return getHost() + ":" + getDiscoveryPort();
    }

    public int getDiscoveryPort() {
        return getMappedPort(DISCOVERY_PORT);
    }

    public int getCommunicationPort() {
        return getMappedPort(COMMUNICATION_PORT);
    }

    private static String newDockerImageName() {
        maybeDownloadAndExtractIgnite();

        final String targetImageName = "invesdwin/ignite-single-node:" + IGNITE_VERSION;
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
                .withFileFromPath(".", IGNITE_CONTAINER_FOLDER.toPath())
                .get();

        return generatedImageName;
    }

    public static File getIgniteHomeFolder() {
        maybeDownloadAndExtractIgnite();
        return IGNITE_HOME_FOLDER;
    }

    private static void maybeDownloadAndExtractIgnite() {
        try {
            final Resource[] resources = PreMergedContext.getInstance()
                    .getResources(
                            "classpath*:/" + Ignite2Container.class.getPackageName().replace(".", "/") + "/files/*");
            for (int i = 0; i < resources.length; i++) {
                final Resource resource = resources[i];
                final File file = new File(IGNITE_CONTAINER_FOLDER, resource.getFilename());
                Files.copyInputStreamToFile(resource.getInputStream(), file);
                if (file.getName().endsWith(".sh")) {
                    file.setExecutable(true, false);
                }
            }

            final File igniteVersionedFolder = new File(IGNITE_CONTAINER_FOLDER,
                    "apache-ignite-" + IGNITE_VERSION + "-bin");
            final File igniteFile = new File(IGNITE_CONTAINER_FOLDER, "apache-ignite-" + IGNITE_VERSION + "-bin.zip");
            if (!igniteFile.exists()) {
                final Instant started = new Instant();
                LOG.info("Started downloading [%s]", igniteFile);
                final File igniteFilePart = new File(igniteFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(igniteFilePart);
                IOUtils.copy(URIs.asUrl("https://archive.apache.org/dist/ignite/" + IGNITE_VERSION + "/apache-ignite-"
                        + IGNITE_VERSION + "-bin.zip"), igniteFilePart);
                Files.moveFileQuietly(igniteFilePart, igniteFile);
                Files.deleteQuietly(IGNITE_HOME_FOLDER);
                Files.deleteQuietly(igniteVersionedFolder);
                LOG.info("Finished downloading [%s] after %s", igniteFile, started);
            }
            if (!IGNITE_HOME_FOLDER.exists()) {
                final Instant started = new Instant();
                LOG.info("Started extracting [%s]", igniteFile);
                final Archiver archiver = ArchiverFactory.createArchiver(igniteFile);
                archiver.extract(igniteFile, IGNITE_CONTAINER_FOLDER);
                Assertions.assertThat(igniteVersionedFolder).exists();
                Files.moveDirectory(igniteVersionedFolder, IGNITE_HOME_FOLDER);

                // Copy custom config files into $IGNITE_HOME/config/
                for (final String filename : new String[] { "custom-config.xml" }) {
                    final File srcConfig = new File(IGNITE_CONTAINER_FOLDER, filename);
                    if (srcConfig.exists()) {
                        Files.copyFile(srcConfig, new File(IGNITE_HOME_FOLDER, "config/" + filename));
                    }
                }
                LOG.info("Finished extracting [%s] after %s", igniteFile, started);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getIgniteVersion() {
        return IGNITE_VERSION;
    }
}