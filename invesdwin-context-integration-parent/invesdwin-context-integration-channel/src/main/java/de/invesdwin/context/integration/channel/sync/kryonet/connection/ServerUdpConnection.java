package de.invesdwin.context.integration.channel.sync.kryonet.connection;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class ServerUdpConnection implements IKryonetConnection {

    private final Server server;

    public ServerUdpConnection(final Server server) {
        this.server = server;
    }

    @Override
    public void close() throws IOException {
        server.stop();
    }

    @Override
    public void send(final IByteBufferProvider message) {
        server.sendToAllUDP(message);
    }

    @Override
    public void addListener(final Listener listener) {
        server.addListener(listener);
    }

    @Override
    public void update() throws IOException {
        server.update(0);
    }

}
