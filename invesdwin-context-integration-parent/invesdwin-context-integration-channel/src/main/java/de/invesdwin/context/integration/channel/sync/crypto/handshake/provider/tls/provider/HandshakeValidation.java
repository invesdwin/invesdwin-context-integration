package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class HandshakeValidation extends AValueObject {

    public static final HandshakeValidation DEFAULT = new HandshakeValidation("Hi Client, I'm Server".getBytes(),
            "Hi Server, I'm Client".getBytes());

    private final IByteBuffer serverPayload;
    private final IByteBuffer clientPayload;

    public HandshakeValidation(final byte[] serverPayload, final byte[] clientPayload) {
        this.serverPayload = ByteBuffers.wrap(serverPayload);
        this.clientPayload = ByteBuffers.wrap(clientPayload);
    }

    public IByteBuffer getServerPayload() {
        return serverPayload;
    }

    public IByteBuffer getClientPayload() {
        return clientPayload;
    }

    /**
     * Adds the application pepper and password to the handshake validator so that no outside application without
     * knowledge of the pepper and password can create a validated handshake with our application. For instance useful
     * when client auth is disabled and we want a different way to authenticate clients without requiring their
     * certificates. This will not help against Man-in-the-Middle-Attacks when the client trusts any server certificate.
     * So beware of always validating one side at minimum.
     */
    //CHECKSTYLE:OFF
    public HandshakeValidation withDerivedPassword(final String password) {
        //CHECKSTYLE:ON
        return withDerivedPassword(CryptoProperties.DEFAULT_PEPPER, password);
    }

    //CHECKSTYLE:OFF
    public HandshakeValidation withDerivedPassword(final byte[] salt, final String password) {
        //CHECKSTYLE:ON
        final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(salt, password.getBytes());
        final byte[] derivedServerPayload = derivedKeyProvider.newDerivedKey(serverPayload.asByteArray(),
                serverPayload.capacity());
        final byte[] derivedClientPayload = derivedKeyProvider.newDerivedKey(clientPayload.asByteArray(),
                clientPayload.capacity());
        return new HandshakeValidation(derivedServerPayload, derivedClientPayload);
    }

}
