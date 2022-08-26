package de.invesdwin.context.integration.channel.sync.kryonet;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Server;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.ByteBufferMessageSerialization;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.ClientTcpConnection;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.ClientUdpConnection;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.IKryonetConnection;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.ServerTcpConnection;
import de.invesdwin.context.integration.channel.sync.kryonet.connection.ServerUdpConnection;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AKryonetSynchronousChannel implements ISynchronousChannel {

    protected final InetAddress address;
    protected final int tcpPort;
    protected final int udpPort;
    protected final boolean server;
    protected IKryonetConnection connection;

    public AKryonetSynchronousChannel(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        this.address = address;
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.server = server;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            final Server server = new Server(16384, 2048, ByteBufferMessageSerialization.INSTANCE);
            server.start();
            final InetSocketAddress tcpAddress;
            if (tcpPort >= 0) {
                tcpAddress = new InetSocketAddress(address, tcpPort);
            } else {
                tcpAddress = null;
            }
            final InetSocketAddress udpAddress;
            if (udpPort >= 0) {
                udpAddress = new InetSocketAddress(address, udpPort);
            } else {
                udpAddress = null;
            }
            server.bind(tcpAddress, udpAddress);
            if (udpPort >= 0) {
                connection = new ServerUdpConnection(server);
            } else {
                connection = new ServerTcpConnection(server);
            }
        } else {
            final Client client = new Client(8192, 2048, ByteBufferMessageSerialization.INSTANCE);
            client.start();
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (true) {
                try {
                    client.connect(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS, address, tcpPort, udpPort);
                    break;
                } catch (final IOException e) {
                    if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e1) {
                            throw new IOException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
            }
            if (udpPort >= 0) {
                connection = new ClientUdpConnection(client);
            } else {
                connection = new ClientTcpConnection(client);
            }
        }
    }

    protected Duration getMaxConnectRetryDelay() {
        return SynchronousChannels.DEFAULT_MAX_RECONNECT_DELAY;
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

}
