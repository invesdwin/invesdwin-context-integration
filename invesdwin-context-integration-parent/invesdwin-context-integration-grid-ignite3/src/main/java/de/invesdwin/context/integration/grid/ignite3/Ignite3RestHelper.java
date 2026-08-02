package de.invesdwin.context.integration.grid.ignite3;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.millis.FDateMillis;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

@Immutable
public final class Ignite3RestHelper {

    private Ignite3RestHelper() {}

    public static void initializeCluster(final String restAddress) {
        initializeCluster(restAddress, null, null);
    }

    public static void initializeCluster(final String restAddress, final String username, final String password) {
        try {
            final String initJson = "{" + "\"metaStorageNodes\": [\"defaultNode\"], "
                    + "\"cmgNodes\": [\"defaultNode\"], " + "\"clusterName\": \"ignite3-test-cluster\"" + "}";

            final HttpClient client = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10)).build();

            final HttpRequest.Builder initRequestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + restAddress + "/management/v1/cluster/init"))
                    .header("Content-Type", "application/json");

            final HttpRequest.Builder stateRequestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + restAddress + "/management/v1/node/state"))
                    .GET();

            // Add HTTP Basic Authentication header to both requests if credentials are provided
            if (username != null && password != null) {
                final String auth = username + ":" + password;
                final String encodedAuth = java.util.Base64.getEncoder()
                        .encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                final String authHeader = "Basic " + encodedAuth;

                initRequestBuilder.header("Authorization", authHeader);
                stateRequestBuilder.header("Authorization", authHeader);
            }

            final HttpRequest request = initRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(initJson)).build();

            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200 && response.statusCode() != 409) {
                throw new RuntimeException("Failed to initialize Ignite 3 cluster. HTTP " + response.statusCode() + ": "
                        + response.body());
            }

            final long timeoutAt = FDateMillis.nowMillis() + 30000;
            final HttpRequest stateRequest = stateRequestBuilder.build();

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

    public static void deployUnitViaRest(final String restAddress, final String unitId, final String version,
            final String filePath) throws Exception {
        deployUnitViaRest(restAddress, unitId, version, filePath, null, null);
    }

    public static void deployUnitViaRest(final String restAddress, final String unitId, final String version,
            final String filePath, final String username, final String password) throws Exception {
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

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + restAddress + "/management/v1/deployment/units/" + unitId + "/" + version))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary);

        // Add HTTP Basic Authentication header if credentials are provided
        if (username != null && password != null) {
            final String auth = username + ":" + password;
            final String encodedAuth = java.util.Base64.getEncoder()
                    .encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            requestBuilder.header("Authorization", "Basic " + encodedAuth);
        }

        final HttpClient httpClient = HttpClient.newHttpClient();
        final HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "Deployment REST call failed: HTTP " + response.statusCode() + " - " + response.body());
        }
    }

}