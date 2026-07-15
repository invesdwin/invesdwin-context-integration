package de.invesdwin.context.integration.channel.axon;

import java.io.Serializable;

import javax.annotation.concurrent.Immutable;

@Immutable
public class AxonChannelMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private byte[] bytes;

    public AxonChannelMessage() {} // Required for serialization

    public AxonChannelMessage(final byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }
}