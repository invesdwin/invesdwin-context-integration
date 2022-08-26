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

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
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
    public boolean isReaderRegistered() {
        //never shut down input or output
        return true;
    }

    @Override
    public boolean isWriterRegistered() {
        //never shut down input or output
        return true;
    }

    /**
     * available() does not work for SSLSocket:
     * https://stackoverflow.com/questions/26320624/how-to-tell-if-java-sslsocket-has-data-available
     */
    @Override
    public boolean isInputStreamAvailableSupported() {
        return false;
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
                final SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket(finalizer.socket,
                        getSocketAddress().getHostName(), getSocketAddress().getPort(), true);
                tlsProvider.configureSocket(sslSocket);
                tlsProvider.onSocketConnected(sslSocket);
                finalizer.socket = sslSocket;
                //use getSocket().startHandshake(); after sending some unencrypted messages (though beware of MITM stripTls attacks)
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    try {
                        finalizer.socket = new Socket();
                        finalizer.socket.connect(socketAddress);
                        final SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket(finalizer.socket,
                                getSocketAddress().getHostName(), getSocketAddress().getPort(), true);
                        tlsProvider.configureSocket(sslSocket);
                        tlsProvider.onSocketConnected(sslSocket);
                        finalizer.socket = sslSocket;
                        //use getSocket().startHandshake(); after sending some unencrypted messages (though beware of MITM stripTls attacks)
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socket.close();
                        finalizer.socket = null;
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
            }
        } else {
            //we directly handshake to establish an encrypted connection
            if (server) {
                final SSLServerSocketFactory serverSocketFactory = context.getServerSocketFactory();
                final SSLServerSocket sslServerSocket = (SSLServerSocket) serverSocketFactory.createServerSocket();
                tlsProvider.configureServerSocket(sslServerSocket);
                finalizer.serverSocket = sslServerSocket;
                finalizer.serverSocket.bind(socketAddress);
                final SSLSocket sslSocket = (SSLSocket) finalizer.serverSocket.accept();
                finalizer.socket = sslSocket;
                tlsProvider.onSocketConnected(sslSocket);
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                final SSLSocketFactory socketFactory = context.getSocketFactory();
                while (true) {
                    try {
                        final SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket();
                        tlsProvider.configureSocket(sslSocket);
                        finalizer.socket = sslSocket;
                        finalizer.socket.connect(socketAddress);
                        tlsProvider.onSocketConnected(sslSocket);
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socket.close();
                        finalizer.socket = null;
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
