package de.invesdwin.context.integration.ignite.test;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.log.Log;

@NotThreadSafe
public class IgniteContainer extends GenericContainer<IgniteContainer> {

    public static final String IGNITE_VERSION = "2.18.0";
    public static final String IGNITE_IMAGE_NAME = "apacheignite/ignite:" + IGNITE_VERSION;
    public static final DockerImageName IGNITE_IMAGE = DockerImageName.parse(IGNITE_IMAGE_NAME);

    public static final int DISCOVERY_PORT = 47500;
    public static final int COMMUNICATION_PORT = 47100;

    private static final Log LOG = new Log(IgniteContainer.class);

    public IgniteContainer() {
        this(IGNITE_IMAGE);
    }

    public IgniteContainer(final DockerImageName image) {
        super(image);

        // Expose Ignite Discovery and Communication Ports
        withExposedPorts(DISCOVERY_PORT, COMMUNICATION_PORT);

        // The official Ignite image starts the server automatically.
        // We wait for the node to complete topology snapshot setup.
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

    public static String getIgniteVersion() {
        return IGNITE_VERSION;
    }
}