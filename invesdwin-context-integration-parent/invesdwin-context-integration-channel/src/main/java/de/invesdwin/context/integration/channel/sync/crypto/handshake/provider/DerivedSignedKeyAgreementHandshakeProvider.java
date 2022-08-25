package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.security.PublicKey;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.DisabledChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.verification.signature.SignatureKey;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.ISignatureAlgorithm;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Instead of using ephemeral signature keys that are encrypted with the pepper, we derive preShared/static signature
 * keys from the pepper to do the authentication and signature. This is also secure against Man-in-the-Middle-Attacks
 * and is simpler than the other approach.
 * 
 * One could also use non-derived preShared/static keys for client and server here.
 */
@Immutable
public class DerivedSignedKeyAgreementHandshakeProvider extends SignedKeyAgreementHandshakeProvider {

    protected DerivedSignedKeyAgreementHandshakeProvider(final AKeyAgreementHandshakeProvider unsignedProvider) {
        super(unsignedProvider);
    }

    /**
     * Could use a password based on session information here to make this more secure.
     */
    @Override
    protected SignatureKey getOurSignatureKey() {
        final ISignatureAlgorithm signatureAlgorithm = getSignatureAlgorithm();
        final DerivedKeyProvider ourDerivedKeyProvider = DerivedKeyProvider
                .fromPassword(CryptoProperties.DEFAULT_PEPPER, "handshake-signature-key");
        final SignatureKey ourSignatureKey = new SignatureKey(signatureAlgorithm, ourDerivedKeyProvider);
        return ourSignatureKey;
    }

    /**
     * Since we derive it anway, there is no need to differentiate between client or server public keys, they are kept
     * secret anyway.
     */
    @Override
    protected PublicKey getOtherVerifyKey(final SignatureKey ourSignatureKey) {
        return ourSignatureKey.getVerifyKey();
    }

    /**
     * No need to encrypt the traffic based on the pepper because we use derived or preShared/static signature keys.
     */
    @Override
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return DisabledChannelFactory.getInstance();
    }

    public static DerivedSignedKeyAgreementHandshakeProvider valueOf(
            final AKeyAgreementHandshakeProvider unsignedProvider) {
        if (unsignedProvider instanceof SignedKeyAgreementHandshakeProvider) {
            return (DerivedSignedKeyAgreementHandshakeProvider) unsignedProvider;
        } else {
            return new DerivedSignedKeyAgreementHandshakeProvider(unsignedProvider);
        }
    }

}
