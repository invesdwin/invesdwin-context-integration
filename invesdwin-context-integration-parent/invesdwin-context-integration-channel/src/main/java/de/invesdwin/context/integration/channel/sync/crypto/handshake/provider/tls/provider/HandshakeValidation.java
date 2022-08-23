package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.bean.AValueObject;

@Immutable
public class HandshakeValidation extends AValueObject {

    public static final HandshakeValidation DEFAULT = new HandshakeValidation("Hi Client, I'm Server".getBytes(),
            "Hi Server, I'm Client".getBytes());

    private final java.nio.ByteBuffer serverPayload;
    private final java.nio.ByteBuffer clientPayload;

    public HandshakeValidation(final byte[] serverPayload, final byte[] clientPayload) {
        this.serverPayload = java.nio.ByteBuffer.wrap(serverPayload);
        this.clientPayload = java.nio.ByteBuffer.wrap(clientPayload);
    }

    public java.nio.ByteBuffer getServerPayload() {
        return serverPayload;
    }

    public java.nio.ByteBuffer getClientPayload() {
        return clientPayload;
    }

    /**
     * Adds the application pepper and password to the handshake validator so that no outside application without
     * knowledge of the pepper and password can create a validated handshake with our application. For instance useful
     * when client auth is disabled and we want a different way to authenticate clients.
     */
    //CHECKSTYLE:OFF
    public HandshakeValidation withDerivedPassword(final String password) {
        //CHECKSTYLE:ON
        final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(CryptoProperties.DEFAULT_PEPPER,
                password.getBytes());
        final byte[] derivedServerPayload = derivedKeyProvider.newDerivedKey(serverPayload.array(),
                serverPayload.capacity());
        final byte[] derivedClientPayload = derivedKeyProvider.newDerivedKey(clientPayload.array(),
                clientPayload.capacity());
        return new HandshakeValidation(derivedServerPayload, derivedClientPayload);
    }

}
