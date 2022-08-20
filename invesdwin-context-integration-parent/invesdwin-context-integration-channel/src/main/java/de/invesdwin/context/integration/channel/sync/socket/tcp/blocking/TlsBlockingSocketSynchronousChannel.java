package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class TlsBlockingSocketSynchronousChannel extends BlockingSocketSynchronousChannel {

    public TlsBlockingSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public SSLSocket getSocket() {
        return (SSLSocket) super.getSocket();
    }

    @Override
    public SSLServerSocket getServerSocket() {
        return (SSLServerSocket) super.getServerSocket();
    }

    @Override
    protected void internalOpen() throws IOException {
        final ITransportLayerSecurityProvider tlsProvider = newTransportLayerSecurityProvider();
        final SSLContext context = tlsProvider.newContext();
        if (tlsProvider.isStartTlsEnabled()) {
            //handshake has to be started manually
            final SSLSocketFactory socketFactory = context.getSocketFactory();
            if (server) {
                //https://deepsec.net/docs/Slides/2014/Java%27s_SSLSocket_-_How_Bad_APIs_Compromise_Security_-_Georg_Lukas.pdf
                finalizer.serverSocket = new ServerSocket();
                finalizer.serverSocket.bind(socketAddress);
                finalizer.socket = finalizer.serverSocket.accept();
                finalizer.socket = socketFactory.createSocket(finalizer.socket, getSocketAddress().getHostName(),
                        getSocketAddress().getPort(), true);
                //use getSocket().startHandshake(); after sending some unencrypted messages (though beware of MITM stripTls attacks)
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    try {
                        finalizer.socket = new Socket();
                        finalizer.socket.connect(socketAddress);
                        finalizer.socket = socketFactory.createSocket(finalizer.socket,
                                getSocketAddress().getHostName(), getSocketAddress().getPort(), true);
                        //use getSocket().startHandshake(); after sending some unencrypted messages (though beware of MITM stripTls attacks)
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socket.close();
                        finalizer.socket = null;
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
        } else {
            //we directly handshake to establish an encrypted connection
            if (server) {
                final SSLServerSocketFactory serverSocketFactory = context.getServerSocketFactory();
                finalizer.serverSocket = serverSocketFactory.createServerSocket();
                finalizer.serverSocket.bind(socketAddress);
                finalizer.socket = finalizer.serverSocket.accept();
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                final SSLSocketFactory socketFactory = context.getSocketFactory();
                while (true) {
                    try {
                        finalizer.socket = socketFactory.createSocket();
                        finalizer.socket.connect(socketAddress);
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socket.close();
                        finalizer.socket = null;
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
        }
        finalizer.socket.setTrafficClass(BlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | BlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
        finalizer.socket.setReceiveBufferSize(socketSize);
        finalizer.socket.setSendBufferSize(socketSize);
        finalizer.socket.setTcpNoDelay(true);
        finalizer.socket.setKeepAlive(true);
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) super.getSocketAddress();
    }

    protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
        return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), server);
    }

}
