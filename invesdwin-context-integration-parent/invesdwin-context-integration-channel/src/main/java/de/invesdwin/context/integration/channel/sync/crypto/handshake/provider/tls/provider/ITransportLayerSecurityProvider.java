package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;

public interface ITransportLayerSecurityProvider {

    ITlsProtocol getProtocol();

    void configureServerSocket(SSLServerSocket socket);

    void configureSocket(SSLSocket socket);

    void onSocketConnected(SSLSocket socket);

    SSLContext newContext();

    SSLEngine newEngine();

    boolean isStartTlsEnabled();

}
