package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.codec.binary.Hex;

import com.nimbusds.srp6.SRP6CryptoParams;
import com.nimbusds.srp6.SRP6VerifierGenerator;
import com.nimbusds.srp6.XRoutine;
import com.nimbusds.srp6.XRoutineWithUserIdentity;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyExchangeHandshakeProvider;
import de.invesdwin.context.security.crypto.key.password.IPasswordHasher;
import de.invesdwin.context.security.crypto.key.password.SimplePasswordHasher;
import de.invesdwin.context.security.crypto.verification.hash.algorithm.DigestAlgorithm;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * https://docs.rs/srp6/1.0.0-alpha.5/srp6/#2-a-session-handshake-for-bob
 * 
 * https://connect2id.com/products/nimbus-srp/usage
 * 
 * This authentication mechanism allows to authenticate a user without making known either the actual userId or
 * password.
 */
@Immutable
public abstract class ASrp6HandshakeProvider extends AKeyExchangeHandshakeProvider {

    public static final XRoutine DEFAULT_X_ROUTINE = new XRoutineWithUserIdentity();
    private SRP6CryptoParams srp6CryptoParams;
    private SRP6VerifierGenerator srp6VerifierGenerator;

    public ASrp6HandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    protected String getSessionIdentifierWithUserId(final String userId) {
        final String sessionIdentifier = super.getSessionIdentifier();
        if (Strings.isBlank(sessionIdentifier)) {
            return userId;
        } else {
            return sessionIdentifier + "-" + userId;
        }
    }

    protected Srp6KeySize getKeySize() {
        return Srp6KeySize.DEFAULT;
    }

    protected String getDigestAlgorithm() {
        return DigestAlgorithm.DEFAULT.getAlgorithm();
    }

    protected final SRP6CryptoParams getSrp6CryptoParams() {
        if (srp6CryptoParams == null) {
            this.srp6CryptoParams = newSrp6CryptoParams();
        }
        return srp6CryptoParams;
    }

    protected SRP6CryptoParams newSrp6CryptoParams() {
        return SRP6CryptoParams.getInstance(getKeySize().getBits(), getDigestAlgorithm());
    }

    protected XRoutine getXRoutine() {
        return DEFAULT_X_ROUTINE;
    }

    protected final SRP6VerifierGenerator getSrp6VerifierGenerator() {
        if (srp6VerifierGenerator == null) {
            srp6VerifierGenerator = newSrp6VerifierGenerator();
        }
        return srp6VerifierGenerator;
    }

    protected SRP6VerifierGenerator newSrp6VerifierGenerator() {
        final SRP6VerifierGenerator verifier = new SRP6VerifierGenerator(getSrp6CryptoParams(),
                CryptoSrp6Routines.INSTANCE);
        verifier.setXRoutine(getXRoutine());
        return verifier;
    }

    protected void waitForMessage(final ASpinWait handshakeReaderSpinWait) {
        try {
            if (!handshakeReaderSpinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
                throw new TimeoutException("Read handshake message timeout exceeded: " + getHandshakeTimeout());
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * We don't send the userId in plaintext per default. This is also used to hash the password (similar to
     * com.dukascopy.auth.client.DukaSRP6ClientSession which uses SHA1; maybe for the small size). We use something like
     * SHA-256 per default which is a fast enough alternative. Using Argon2/PBKDF2/SCrypt/BCrypt here would be a bit
     * overkill I guess. Since we anyway only send the userId and do that normally over an encrypted channel.
     * 
     * Replace this method with a no-op to send the plaintext userId to the server.
     */
    protected String hashSecret(final String secret) {
        final IPasswordHasher hasher = getPasswordHasher();
        final String encodedHex = Hex
                .encodeHexString(hasher.hash(Bytes.EMPTY_ARRAY, secret.getBytes(), hasher.getDefaultHashLength()));
        return encodedHex;
    }

    protected SimplePasswordHasher getPasswordHasher() {
        return SimplePasswordHasher.DEFAULT;
    }

    /**
     * We use an encrypted and verified handshake channel to make sure no-one in the middle can read the (hashed) userId
     * over the wire. Though as long as the the userId is sent in a strong hashed form, then DisabledChannelFactory
     * could be used to send the hash in plaintext.
     */
    @Override
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return super.newAuthenticatedHandshakeChannelFactory();
    }

}
