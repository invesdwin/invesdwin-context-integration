package de.invesdwin.context.integration.channel.sync.kryonet.connection;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class ClientUdpConnection implements IKryonetConnection {

    private final Client client;

    public ClientUdpConnection(final Client client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void send(final IByteBufferProvider message) {
        client.sendUDP(message);
    }

    @Override
    public void addListener(final Listener listener) {
        client.addListener(listener);
    }

    @Override
    public void update() throws IOException {
        client.update(0);
    }

}
