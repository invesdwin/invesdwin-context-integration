package de.invesdwin.context.integration.ignite3.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
import de.invesdwin.util.time.date.millis.FDateMillis;

@NotThreadSafe
public class Ignite3Container extends GenericContainer<Ignite3Container> {

    public static final String IGNITE_VERSION = "3.1.0";

    public static final int CLIENT_PORT = 10800;
    public static final int REST_PORT = 10300;

    private static final Log LOG = new Log(Ignite3Container.class);

    private static final File IGNITE_CONTAINER_FOLDER = new File(ContextProperties.getHomeDataDirectory(),
            Ignite3Container.class.getSimpleName());
    private static final File IGNITE_HOME_FOLDER = new File(IGNITE_CONTAINER_FOLDER, "ignite");
    private static final boolean FORCE_BUILD_DOCKER_IMAGE = false;

    public Ignite3Container() {
        super(DockerImageName.parse(newDockerImageName()));
        addFixedExposedPort(CLIENT_PORT, CLIENT_PORT);
        addFixedExposedPort(REST_PORT, REST_PORT);
        waitingFor(Wait.forLogMessage(".*REST server started successfully.*", 1));
    }

    private static String newDockerImageName() {
        maybeDownloadAndExtractIgnite();

        final String targetImageName = "invesdwin/ignite3-single-node:" + IGNITE_VERSION;
        if (FORCE_BUILD_DOCKER_IMAGE) {
            LOG.info("Forcing build of Docker image [%s]...", targetImageName);
            return buildDockerImage(targetImageName);
        } else {
            final DockerClient dockerClient = DockerClientFactory.lazyClient();
            try {
                dockerClient.inspectImageCmd(targetImageName).exec();
                LOG.info("Found cached Docker image: [%s]. Skipping build.", targetImageName);
                return targetImageName;
            } catch (final NotFoundException e) {
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

    private static void maybeDownloadAndExtractIgnite() {
        try {
            // Extract files from classpath
            final Resource[] resources = PreMergedContext.getInstance()
                    .getResources(
                            "classpath*:/" + Ignite3Container.class.getPackageName().replace(".", "/") + "/files/*");
            for (int i = 0; i < resources.length; i++) {
                final Resource resource = resources[i];
                final File file = new File(IGNITE_CONTAINER_FOLDER, resource.getFilename());
                Files.copyInputStreamToFile(resource.getInputStream(), file);
                if (file.getName().endsWith(".sh")) {
                    file.setExecutable(true, false);
                }
            }

            final File igniteVersionedFolder = new File(IGNITE_CONTAINER_FOLDER, "ignite3-db-" + IGNITE_VERSION);
            final File igniteFile = new File(IGNITE_CONTAINER_FOLDER, "ignite3-db-" + IGNITE_VERSION + ".zip");

            if (!igniteFile.exists()) {
                final Instant started = new Instant();
                LOG.info("Started downloading [%s]", igniteFile);
                final File igniteFilePart = new File(igniteFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(igniteFilePart);
                IOUtils.copy(URIs.asUrl("https://archive.apache.org/dist/ignite/" + IGNITE_VERSION + "/ignite3-db-"
                        + IGNITE_VERSION + ".zip"), igniteFilePart);
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

                // Copy custom config file into $IGNITE_HOME/etc/
                for (final String filename : new String[] { "ignite-config.conf" }) {
                    final File srcConfig = new File(IGNITE_CONTAINER_FOLDER, filename);
                    if (srcConfig.exists()) {
                        Files.copyFile(srcConfig, new File(IGNITE_HOME_FOLDER, "etc/" + filename));
                    }
                }
                LOG.info("Finished extracting [%s] after %s", igniteFile, started);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void containerIsStarted(final com.github.dockerjava.api.command.InspectContainerResponse containerInfo) {
        initializeCluster();
    }

    private void initializeCluster() {
        final String restHost = getHost();
        final int restPort = getMappedPort(REST_PORT);

        try {
            final String initJson = "{" + "\"metaStorageNodes\": [\"defaultNode\"], "
                    + "\"cmgNodes\": [\"defaultNode\"], " + "\"clusterName\": \"ignite3-test-cluster\"" + "}";

            final HttpClient client = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10)).build();

            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + restHost + ":" + restPort + "/management/v1/cluster/init"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(initJson))
                    .build();

            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200 && response.statusCode() != 409) {
                throw new RuntimeException("Failed to initialize Ignite 3 cluster. HTTP " + response.statusCode() + ": "
                        + response.body());
            }

            final long timeoutAt = FDateMillis.nowMillis() + 30000;
            final HttpRequest stateRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + restHost + ":" + restPort + "/management/v1/node/state"))
                    .GET()
                    .build();

            while (FDateMillis.nowMillis() < timeoutAt) {
                try {
                    final HttpResponse<String> stateResp = client.send(stateRequest,
                            HttpResponse.BodyHandlers.ofString());
                    if (stateResp.statusCode() == 200 && stateResp.body().contains("\"state\":\"STARTED\"")) {
                        return;
                    }
                } catch (final Exception ignored) {
                }
                Thread.sleep(500);
            }
            throw new RuntimeException(
                    "Timed out waiting for Ignite 3 cluster to reach STARTED state after initialization.");

        } catch (final Exception e) {
            throw new RuntimeException("Error during Ignite 3 cluster initialization", e);
        }
    }

    public String getClientAddress() {
        return getHost() + ":" + getMappedPort(CLIENT_PORT);
    }
}