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

    /**
     * This is only a hint for the underlying transport implementation (e.g. blocking socket or netty). For handshaker
     * usage the negotiation begins as soon as the channel is opened. Use the underlying reader/writer to send
     * unencrypted payloads.
     */
    boolean isStartTlsEnabled();

    /**
     * A hint to the handshaker to send some payloads bidirectional to check if the communication works properly after
     * the handshake. For debugging purposes. Though could also be used to validate if a re-handshaking got attacked by
     * a Man-in-the-Middle that got access to the client certificate.
     * 
     * This is not supported by netty or blocking socket based handshaking implementations.
     * 
     * Though this will not help when certificates or certificate validation on both sides is disabled. In that case
     * this only prevents other applications from entering, but does not prevent a Man-in-the-Middle-Attack from
     * impersonating both sides by interchanging their respective certificates.
     * (https://stackoverflow.com/questions/50033054/can-a-man-in-the-middle-attack-on-an-https-read-all-the-communication)
     */
    HandshakeValidation getHandshakeValidation();

}
