package de.invesdwin.context.integration.channel.redis;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

@NotThreadSafe
public class RedisContainer extends ARespContainer<RedisContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("redis:8.8.3-alpine");

    public RedisContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public RedisContainer(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public RedisContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected String[] newCommand(final int port) {
        return new String[] { "redis-server", "--port", String.valueOf(port), "--save", "", "--appendonly", "no" };
    }
}
