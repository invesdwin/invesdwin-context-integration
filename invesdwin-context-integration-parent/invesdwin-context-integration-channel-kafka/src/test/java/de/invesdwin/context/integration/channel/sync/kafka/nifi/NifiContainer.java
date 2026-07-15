package de.invesdwin.context.integration.channel.sync.kafka.nifi;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.invesdwin.context.integration.marshaller.MarshallerJsonJackson;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;
import de.invesdwin.util.lang.uri.connect.apache.HttpEntities;
import de.invesdwin.util.lang.uri.ssl.DisabledX509Trustmanager;

@NotThreadSafe
public class NifiContainer extends GenericContainer<NifiContainer> {

    private final Log log = new Log(this);

    public NifiContainer() {
        this("apache/nifi:2.4.0");
    }

    public NifiContainer(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public NifiContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        final int nifiHttpPort = NetworkUtil.findAvailableTcpPort();
        final List<String> portBindings = new ArrayList<>();
        portBindings.add(nifiHttpPort + ":" + nifiHttpPort);
        withEnv("NIFI_WEB_HTTPS_PORT", String.valueOf(nifiHttpPort));
        withEnv("SINGLE_USER_CREDENTIALS_USERNAME", "admin");
        withEnv("SINGLE_USER_CREDENTIALS_PASSWORD", "adminadminadmin");
        withExposedPorts(nifiHttpPort);
        setPortBindings(portBindings);
    }

    @Override
    public void start() {
        super.start();
        log.info("NiFi website: %s", getNifiWebsiteUrl());
    }

    public void setupFlow(final String flowJson) throws IOException, JsonProcessingException, JsonMappingException {
        final String nifiRestApi = getNifiRestApiUrl(); //nifi rest api url

        final String authToken = getAuthToken(nifiRestApi);
        //log.info("Auth Token obtained.");

        //log.info(patchedJsonStr);

        final String rootGroupUrl = nifiRestApi + "/flow/process-groups/root";
        final String rootGroupJson = getProcessGroupID(authToken, rootGroupUrl);
        final ObjectMapper mapper = MarshallerJsonJackson.getInstance().getJsonMapper(false);
        final JsonNode rootGroupNode = mapper.readTree(rootGroupJson);
        final String rootGroupId = rootGroupNode.path("processGroupFlow").path("id").asText();
        //log.info("Root Process Group ID = " + rootGroupId);

        final MultipartEntityBuilder multipartEntityBuilder = multipartEntityBuilderCustomization(flowJson,
                "nifiFlow.json");
        final HttpEntity multipartEntity = multipartEntityBuilder.build();
        final String uploadResponse = uploadFlow(nifiRestApi, authToken, rootGroupId, multipartEntity);

        //log.info("Flow uploaded.");

        final JsonNode newProcessGroupNode = mapper.readTree(uploadResponse);
        final String newProcessGroupId = newProcessGroupNode.path("id").asText();
        final String controllerServicesJson = getControllerServices(nifiRestApi, authToken, newProcessGroupId);

        final JsonNode services = mapper.readTree(controllerServicesJson).path("controllerServices");
        for (final JsonNode service : services) {
            enableControllerService(nifiRestApi, authToken, mapper, service);
        }
        final JsonNode revisionNode = getLatestRevision(nifiRestApi, authToken, mapper, newProcessGroupId);
        startFlow(nifiRestApi, authToken, newProcessGroupId, revisionNode);
    }

    public String getNifiServerAddress() {
        return "https://localhost:" + getExposedPorts().get(0);
    }

    public String getNifiRestApiUrl() {
        return getNifiServerAddress() + "/nifi-api";
    }

    public String getNifiWebsiteUrl() {
        return getNifiServerAddress() + "/nifi/";
    }

    protected JsonNode getLatestRevision(final String nifiRestApi, final String authToken, final ObjectMapper mapper,
            final String newProcessGroupId) throws IOException, JsonProcessingException, JsonMappingException {
        final String latestRevisionURL = nifiRestApi + "/flow/process-groups/" + newProcessGroupId;
        final String latestRevisionID = URIs.connect(latestRevisionURL)
                .putHeader("Authorization", "Bearer " + authToken)
                .setMethod(IURIsConnect.GET)
                .setContentType("application/json")
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .downloadThrowing();

        final JsonNode latestRevisionNode = mapper.readTree(latestRevisionID);
        final JsonNode revisionNode = latestRevisionNode.path("revision");
        return revisionNode;
    }

    protected void startFlow(final String nifiRestApi, final String authToken, final String newProcessGroupId,
            final JsonNode revisionNode) throws IOException {
        final String startPayload = TextDescription.format(
                "{\"id\":\"%s\",\"state\":\"RUNNING\",\"revision\":%s,\"disconnectedNodeAcknowledged\":true}",
                newProcessGroupId, revisionNode.toString());
        final String pgFlowUrl = nifiRestApi + "/flow/process-groups/" + newProcessGroupId;
        URIs.connect(pgFlowUrl)
                .setMethod(IURIsConnect.PUT)
                .putHeader("Authorization", "Bearer " + authToken)
                .setContentType("application/json")
                .setBody(startPayload)
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .downloadThrowing();
    }

    protected void enableControllerService(final String nifiRestApi, final String authToken, final ObjectMapper mapper,
            final JsonNode service) throws IOException {
        final String serviceId = service.path("component").path("id").asText();
        final String revisionVersion = service.path("revision").path("version").asText();

        final ObjectNode enableRequest = mapper.createObjectNode();
        final ObjectNode revisionNode = enableRequest.putObject("revision");
        revisionNode.put("version", Integer.parseInt(revisionVersion));

        final ObjectNode componentNode = enableRequest.putObject("component");
        componentNode.put("id", serviceId);
        componentNode.put("state", "ENABLED");

        final String enableUrl = nifiRestApi + "/controller-services/" + serviceId;

        try {
            URIs.connect(enableUrl)
                    .putHeader("Authorization", "Bearer " + authToken)
                    .setMethod(IURIsConnect.PUT)
                    .setContentType("application/json")
                    .setBody(enableRequest.toString())
                    .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                    .downloadThrowing();
        } catch (final Throwable t) {
            log.error("Unable to start controller service %s. Maybe it is stateless? %s", serviceId, t.toString());
        }
    }

    protected String getControllerServices(final String nifiRestApi, final String authToken,
            final String newProcessGroupId) throws IOException {
        final String controllerServicesJson = URIs
                .connect(nifiRestApi + "/flow/process-groups/" + newProcessGroupId + "/controller-services")
                .setMethod(IURIsConnect.GET)
                .putHeader("includeReferencingComponents", "true")
                .putHeader("Authorization", "Bearer " + authToken)
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .downloadThrowing();
        return controllerServicesJson;
    }

    protected String uploadFlow(final String nifiRestApi, final String authToken, final String rootGroupId,
            final HttpEntity multipartEntity) throws IOException {
        final String uploadUrl = nifiRestApi + "/process-groups/" + rootGroupId + "/process-groups/upload";

        final String uploadResponse = URIs.connect(uploadUrl)
                .putHeader("Authorization", "Bearer " + authToken)
                .setContentType(multipartEntity.getContentType())
                .setMethod(IURIsConnect.POST)
                .setBody(HttpEntities.getContent(multipartEntity))
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .downloadThrowing();
        return uploadResponse;
    }

    protected MultipartEntityBuilder multipartEntityBuilderCustomization(final String patchedJsonStr,
            final String fileName) {
        final MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        multipartEntityBuilder.addBinaryBody("file", patchedJsonStr.getBytes(StandardCharsets.UTF_8),
                ContentType.APPLICATION_JSON, fileName + ".json");
        multipartEntityBuilder.addTextBody("groupName", fileName);
        multipartEntityBuilder.addTextBody("clientId", "4036074c-018c-1000-3e06-aaaaaaaaaaaa");
        multipartEntityBuilder.addTextBody("disconnectNode", "true");
        multipartEntityBuilder.addTextBody("positionX", "0");
        multipartEntityBuilder.addTextBody("positionY", "0");
        return multipartEntityBuilder;
    }

    protected String getProcessGroupID(final String authToken, final String rootGroupUrl) throws IOException {
        return URIs.connect(rootGroupUrl)
                .setMethod(IURIsConnect.GET)
                .putHeader("Authorization", "Bearer " + authToken)
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .downloadThrowing();
    }

    protected String getAuthToken(final String nifiRestApi) throws IOException {
        return URIs.connect(nifiRestApi + "/access/token")
                .setMethod(IURIsConnect.POST)
                .setTrustManager(DisabledX509Trustmanager.INSTANCE)
                .setBody("username=admin&password=adminadminadmin")
                .setContentType("application/x-www-form-urlencoded")
                .downloadThrowing();
    }

}
