package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import javax.annotation.concurrent.Immutable;

import com.nimbusds.srp6.SRP6Routines;

import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;

@Immutable
public final class CryptoSrp6Routines extends SRP6Routines {

    public static final CryptoSrp6Routines INSTANCE = new CryptoSrp6Routines();

    private CryptoSrp6Routines() {
        random = null;
    }

    @Override
    public byte[] generateRandomSalt(final int numBytes) {
        return super.generateRandomSalt(numBytes, CryptoRandomGenerators.getThreadLocalCryptoRandom());
    }

}
