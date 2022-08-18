package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyAgreementHandshakeProvider;
import de.invesdwin.context.security.crypto.encryption.cipher.asymmetric.algorithm.RsaKeySize;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DhHandshakeProvider extends AKeyAgreementHandshakeProvider {

    public DhHandshakeProvider(final Duration handshakeTimeout) {
        super(handshakeTimeout);
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
