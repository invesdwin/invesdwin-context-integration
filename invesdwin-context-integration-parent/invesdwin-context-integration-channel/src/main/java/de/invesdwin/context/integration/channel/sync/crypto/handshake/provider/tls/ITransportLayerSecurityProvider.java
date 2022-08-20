package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

public interface ITransportLayerSecurityProvider {

    void configureServerSocket(SSLServerSocket socket);

    void configureSocket(SSLSocket socket);

    void onSocketConnected(SSLSocket socket);

    SSLContext newContext();

    SSLEngine newEngine();

    boolean isStartTlsEnabled();

}
