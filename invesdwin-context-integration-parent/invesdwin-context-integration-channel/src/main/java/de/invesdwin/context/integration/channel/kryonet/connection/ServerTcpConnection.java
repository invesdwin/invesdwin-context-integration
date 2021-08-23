package de.invesdwin.context.integration.channel.kryonet.connection;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;

import de.invesdwin.context.integration.channel.command.ISynchronousCommand;

@Immutable
public class ServerTcpConnection implements IKryonetConnection {

    private final Server server;

    public ServerTcpConnection(final Server server) {
        this.server = server;
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    @Override
    public void send(final ISynchronousCommand<byte[]> message) {
        server.sendToAllTCP(message);
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
