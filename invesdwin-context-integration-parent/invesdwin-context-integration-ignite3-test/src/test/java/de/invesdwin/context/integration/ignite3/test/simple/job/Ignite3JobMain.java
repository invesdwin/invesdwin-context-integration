package de.invesdwin.context.integration.ignite3.test.simple.job;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.util.time.date.millis.FDateMillis;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

@NotThreadSafe
public class Ignite3JobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-m", aliases = "--master", usage = "Defines the Ignite master address", required = true)
    protected String master;
    @Option(name = "--rest", usage = "Defines the Ignite REST address", required = true)
    protected String restAddress;
    @Option(name = "-j", aliases = "--jobJar", usage = "Defines the job JAR path", required = true)
    protected String jobJar;

    public Ignite3JobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        try {
            final String fixedMaster = master != null ? master.replace("localhost", "127.0.0.1") : master;
            final String fixedRestAddress = restAddress != null ? restAddress.replace("localhost", "127.0.0.1")
                    : restAddress;

            final String unitId = "simple-job-unit";
            final String unitVersion = "3.0.0";
            deployUnitViaRest(fixedRestAddress, unitId, unitVersion, jobJar);

            final DeploymentUnit unit = new DeploymentUnit(unitId, unitVersion);

            try (IgniteClient client = IgniteClient.builder().addresses(fixedMaster).build()) {
                client.sql()
                        .execute(null,
                                "CREATE TABLE IF NOT EXISTS ignite3JobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

                final JobDescriptor<String, Ignite3Task.TaskResult> descriptor = JobDescriptor.<String, Ignite3Task.TaskResult> builder(
                        Ignite3Task.class.getName())
                        .resultClass(Ignite3Task.TaskResult.class)
                        .units(List.of(unit))
                        .build();

                final JobTarget target = JobTarget.anyNode(client.clusterNodes());
                final List<CompletableFuture<Ignite3Task.TaskResult>> futures = new ArrayList<>();

                for (int rank = 0; rank < size; rank++) {
                    final String args = rank + ";" + size + ";" + fixedMaster;
                    final CompletableFuture<Ignite3Task.TaskResult> future = client.compute()
                            .submitAsync(target, descriptor, args)
                            .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);
                    futures.add(future);
                }

                final File targetDir = parseLogDirectory(logDir);
                for (final CompletableFuture<Ignite3Task.TaskResult> future : futures) {
                    final Ignite3Task.TaskResult result = future.join();
                    final File logFile = new File(targetDir, result.getLogFileName());
                    de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(),
                            StandardCharsets.UTF_8);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to run Ignite 3 job", e);
        }
    }

    private void deployUnitViaRest(final String restAddress, final String unitId, final String version,
            final String filePath) throws Exception {
        final String boundary = "---Ignite3Boundary" + FDateMillis.nowMillis();
        final Path path = Path.of(filePath);
        final byte[] fileBytes = java.nio.file.Files.readAllBytes(path);

        final String header = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"unitContent\"; filename=\"" + path.getFileName().toString()
                + "\"\r\n" + "Content-Type: application/java-archive\r\n\r\n";
        final String footer = "\r\n--" + boundary + "--\r\n";

        final FastByteArrayOutputStream body = new FastByteArrayOutputStream();
        body.write(header.getBytes(StandardCharsets.UTF_8));
        body.write(fileBytes);
        body.write(footer.getBytes(StandardCharsets.UTF_8));

        final HttpClient httpClient = HttpClient.newHttpClient();
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + restAddress + "/management/v1/deployment/units/" + unitId + "/" + version))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "Deployment REST call failed: HTTP " + response.statusCode() + " - " + response.body());
        }
    }

    private static File parseLogDirectory(final String logDir) {
        if (logDir.startsWith("file:")) {
            return new File(URI.create(logDir));
        }
        return new File(logDir);
    }

    public static void main(final String[] args) {
        new Ignite3JobMain(args).run();
    }
}