package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyAgreementHandshakeProvider;
import de.invesdwin.context.security.crypto.encryption.cipher.asymmetric.algorithm.RsaKeySize;
import de.invesdwin.util.time.duration.Duration;

/**
 * In its default configuration, this is actually an authenticated (with pre shared password+pepper based encryption)
 * ephemeral diffie hellman handshake (authenticated EDH/DHE).
 * 
 * WARNING: Prefer the SignedEcdhHandshakeProvider where possible.
 */
@NotThreadSafe
public class DhHandshakeProvider extends AKeyAgreementHandshakeProvider {

    public DhHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    @Override
    public String getKeyAgreementAlgorithm() {
        return "DH";
    }

    @Override
    public String getKeyAlgorithm() {
        return getKeyAgreementAlgorithm();
    }

    @Override
    public int getKeySizeBits() {
        return RsaKeySize.DEFAULT.getBits();
    }

}
