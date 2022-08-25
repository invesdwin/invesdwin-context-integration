package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.io.IOException;
import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import com.nimbusds.srp6.SRP6VerifierGenerator;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupInput;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupOutput;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public class PreSharedSrp6ServerHandshakeProvider extends ASrp6ServerHandshakeProvider {

    private final String userIdHash;
    private final String passwordHash;

    /**
     * SessionIdentifier can be null, in that case only the userId is used as the salt for the DerivedKeyProvider.
     */
    public PreSharedSrp6ServerHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier,
            final String userId, final String password) {
        super(handshakeTimeout, sessionIdentifier);
        this.userIdHash = hashSecret(userId);
        this.passwordHash = hashSecret(password);
    }

    /**
     * Override this method with something that actually looks up user credentials that are stored from a previous
     * registration process. In the default implementation we just authenticate against the pre shared pepper. Though
     * this is a bad idea for applications that run on client computers.
     */
    @Override
    protected Srp6ServerStep1LookupOutput getServerStep1LookupResult(final Srp6ServerStep1LookupInput input)
            throws IOException {
        if (!input.getUserIdHash().equals(userIdHash)) {
            throw new IOException("Bad client credentials");
        }
        final SRP6VerifierGenerator srp6VerifierGenerator = getSrp6VerifierGenerator();
        final BigInteger passwordSaltS = new BigInteger(getPasswordHasher().getDefaultHashLength(),
                CryptoRandomGenerators.getThreadLocalCryptoRandom());
        final BigInteger passwordVerifierV = srp6VerifierGenerator.generateVerifier(passwordSaltS,
                input.getUserIdHash(), passwordHash);
        return new Srp6ServerStep1LookupOutput(passwordSaltS, passwordVerifierV);
    }

}
