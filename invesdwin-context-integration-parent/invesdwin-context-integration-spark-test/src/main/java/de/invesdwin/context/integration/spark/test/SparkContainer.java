package de.invesdwin.context.integration.spark.test;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.Instant;

@NotThreadSafe
public class SparkContainer extends GenericContainer<SparkContainer> {

    private static final int WEBUI_HTTP_PORT = 8080;
    private static final int MASTER_PORT = 7077;

    private static final Log LOG = new Log(SparkContainer.class);

    private static final String SPARK_VERSION = "4.2.0";
    private static final DockerImageName SPARK_IMAGE = DockerImageName.parse("apache/spark:" + SPARK_VERSION);

    private static final String SPARK_PACKAGE = "spark-" + SPARK_VERSION + "-bin-hadoop3";
    private static final File SPARK_CONTAINER_FOLDER = new File(ContextProperties.getHomeDataDirectory(),
            SparkContainer.class.getSimpleName());
    private static final File SPARK_HOME_FOLDER = new File(SPARK_CONTAINER_FOLDER, SPARK_PACKAGE);

    public SparkContainer() {
        super(SPARK_IMAGE);

        // Expose Spark Master Port and Web UI
        withExposedPorts(MASTER_PORT, WEBUI_HTTP_PORT);

        // Launch Master in background, then launch Worker connected to local Master
        withCommand("/bin/sh", "-c",
                "/opt/spark/bin/spark-class " + org.apache.spark.deploy.master.Master.class.getName()
                        + " --host 0.0.0.0 & " + "/opt/spark/bin/spark-class "
                        + org.apache.spark.deploy.worker.Worker.class.getName() + " spark://127.0.0.1:" + MASTER_PORT);

        // Wait for the Web UI to be HTTP 200 OK
        waitingFor(Wait.forHttp("/").forPort(WEBUI_HTTP_PORT));
    }

    @Override
    public void start() {
        super.start();
        LOG.warn("Spark Web UI available at: " + getWebUiUrl());
    }

    public String getMasterUrl() {
        return "spark://" + getHost() + ":" + getMappedPort(MASTER_PORT);
    }

    public String getWebUiUrl() {
        return "http://" + getHost() + ":" + getMappedPort(WEBUI_HTTP_PORT);
    }

    public static String getSparkVersion() {
        return SPARK_VERSION;
    }

    public static File getSparkHomeFolder() {
        maybeDownloadAndExtractSpark();
        return SPARK_HOME_FOLDER;
    }

    private static void maybeDownloadAndExtractSpark() {
        try {
            final File sparkArchiveFile = new File(SPARK_CONTAINER_FOLDER, SPARK_PACKAGE + ".tgz");

            if (!sparkArchiveFile.exists()) {
                final Instant started = new Instant();
                LOG.info("Started downloading [%s]", sparkArchiveFile);
                Files.forceMkdirParent(sparkArchiveFile);
                final File sparkArchiveFilePart = new File(sparkArchiveFile.getAbsolutePath() + ".part");
                Files.deleteQuietly(sparkArchiveFilePart);

                final String downloadUrl = "https://archive.apache.org/dist/spark/spark-" + SPARK_VERSION + "/"
                        + SPARK_PACKAGE + ".tgz";
                IOUtils.copy(URIs.asUrl(downloadUrl), sparkArchiveFilePart);

                Files.moveFileQuietly(sparkArchiveFilePart, sparkArchiveFile);
                Files.deleteQuietly(SPARK_HOME_FOLDER);
                LOG.info("Finished downloading [%s] after %s", sparkArchiveFile, started);
            }

            if (!SPARK_HOME_FOLDER.exists()) {
                final Instant started = new Instant();
                LOG.info("Started extracting [%s]", sparkArchiveFile);

                final Archiver archiver = ArchiverFactory.createArchiver(sparkArchiveFile);
                archiver.extract(sparkArchiveFile, SPARK_CONTAINER_FOLDER);

                Assertions.assertThat(SPARK_HOME_FOLDER).exists();
                LOG.info("Finished extracting [%s] after %s", sparkArchiveFile, started);
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to provision local SPARK_HOME", e);
        }
    }
}