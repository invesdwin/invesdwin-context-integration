package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@NotThreadSafe
public class SchemaRegistryContainer extends net.christophschubert.cp.testcontainers.SchemaRegistryContainerAccessor {

    public SchemaRegistryContainer(final ConfluentServerContainer bootstrap, final Network network) {
        super(DockerImageName
                .parse(ConfluentServerContainer.REPOSITORY + "/cp-schema-registry:" + ConfluentServerContainer.TAG),
                bootstrap, network);
    }

    public String getSchemaRegistryUrl() {
        return getBaseUrl();
    }
}
