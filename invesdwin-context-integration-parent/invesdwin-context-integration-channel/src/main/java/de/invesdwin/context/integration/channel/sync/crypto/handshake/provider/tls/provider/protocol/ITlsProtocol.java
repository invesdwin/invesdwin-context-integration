package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

public interface ITlsProtocol {

    String getName();

    String getFamily();

    boolean isHandshakeTimeoutRecoveryEnabled();

    boolean isVersioned();

}
