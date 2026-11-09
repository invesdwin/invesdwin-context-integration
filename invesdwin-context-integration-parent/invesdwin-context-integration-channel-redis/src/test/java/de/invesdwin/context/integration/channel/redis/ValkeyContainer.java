package de.invesdwin.context.integration.channel.redis;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

@NotThreadSafe
public class ValkeyContainer extends ARespContainer<ValkeyContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("valkey/valkey:9.1.3");

    public ValkeyContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public ValkeyContainer(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public ValkeyContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected String[] newCommand(final int port) {
        return new String[] { "valkey-server", "--port", String.valueOf(port), "--save", "", "--appendonly", "no" };

    }
}
