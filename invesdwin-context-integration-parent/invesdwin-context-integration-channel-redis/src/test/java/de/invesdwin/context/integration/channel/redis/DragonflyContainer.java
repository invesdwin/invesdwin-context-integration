package de.invesdwin.context.integration.channel.redis;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

@NotThreadSafe
public class DragonflyContainer extends ARespContainer<DragonflyContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("dragonflydb/dragonfly:v1.27.1");

    public DragonflyContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public DragonflyContainer(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public DragonflyContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected String[] newCommand(final int port) {
        return new String[] { "dragonfly", "--logtostderr", "--port", String.valueOf(port), "--dbfilename=" };
    }

}
