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

    /**
     * A hint to the handshaker to send some payloads bidirectional to check if the communication works properly after
     * the handshake. For debugging purposes. Though could also be used to validate if a re-handshaking got attacked by
     * a Man-in-the-Middle that got access to the client certificate.
     */
    HandshakeValidation getHandshakeValidation();

}
