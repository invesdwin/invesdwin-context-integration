package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyAgreementHandshakeProvider;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaAlgorithm;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaKeySize;
import de.invesdwin.util.time.duration.Duration;

/**
 * In its default configuration, this is actually an authenticated (with pre shared password+pepper based encryption)
 * ephemeral elliptic curve diffie hellman handshake (authenticated ECDHE).
 * 
 * WARNING: Prefer the SignedEcdhHandshakeProvider where possible.
 */
@NotThreadSafe
public class EcdhHandshakeProvider extends AKeyAgreementHandshakeProvider {

    public EcdhHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    @Override
    public String getKeyAgreementAlgorithm() {
        return "ECDH";
    }

    @Override
    public String getKeyAlgorithm() {
        return EcdsaAlgorithm.DEFAULT.getKeyAlgorithm();
    }

    @Override
    public int getKeySizeBits() {
        return EcdsaKeySize.DEFAULT.getBits();
    }

}