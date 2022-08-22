package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum ClientAuth {

    /**
     * Client authentication is required
     */
    NEED,

    /**
     * Client authentication is requested but not required
     */
    WANT,

    /**
     * Client authentication is not performed
     */
    NONE
}