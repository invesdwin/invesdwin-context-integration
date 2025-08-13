package net.christophschubert.cp.testcontainers;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@SuppressWarnings("deprecation")
@NotThreadSafe
public class SchemaRegistryContainerAccessor extends net.christophschubert.cp.testcontainers.SchemaRegistryContainer {

    public SchemaRegistryContainerAccessor(final DockerImageName imageName, final KafkaContainer bootstrap,
            final Network network) {
        super(imageName, bootstrap, network);
    }

    public KafkaContainer getBootstrap() {
        return bootstrap;
    }

}
