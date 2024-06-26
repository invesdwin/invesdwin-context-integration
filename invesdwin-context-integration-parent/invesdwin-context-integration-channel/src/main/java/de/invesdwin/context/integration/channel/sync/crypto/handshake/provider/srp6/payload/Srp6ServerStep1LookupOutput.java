package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;

@Immutable
public class Srp6ServerStep1LookupOutput extends AValueObject {

    private final BigInteger passwordSaltS;
    private final BigInteger passwordVerifierV;

    public Srp6ServerStep1LookupOutput(final BigInteger passwordSaltS, final BigInteger passwordVerifierV) {
        this.passwordSaltS = passwordSaltS;
        this.passwordVerifierV = passwordVerifierV;
    }

    public BigInteger getPasswordSaltS() {
        return passwordSaltS;
    }

    public BigInteger getPasswordVerifierV() {
        return passwordVerifierV;
    }

}
