package de.invesdwin.context.integration.channel.sync.kryonet.connection;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@Immutable
public class ClientTcpConnection implements IKryonetConnection {

    private final Client client;

    public ClientTcpConnection(final Client client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void send(final IByteBufferWriter message) {
        client.sendTCP(message);
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
