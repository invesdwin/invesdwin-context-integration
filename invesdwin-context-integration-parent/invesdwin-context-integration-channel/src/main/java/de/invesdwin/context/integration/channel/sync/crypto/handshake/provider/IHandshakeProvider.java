package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.util.time.duration.Duration;

public interface IHandshakeProvider {

    void handshake(HandshakeChannel channel) throws IOException;

    Duration getHandshakeTimeout();

}
