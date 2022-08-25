package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;

@Immutable
public class Srp6ServerStep1Result extends AValueObject {

    private final BigInteger passwordSaltS;
    private final BigInteger serverPublicValueB;

    public Srp6ServerStep1Result(final BigInteger passwordSaltS, final BigInteger serverPublicValueB) {
        this.passwordSaltS = passwordSaltS;
        this.serverPublicValueB = serverPublicValueB;
    }

    public BigInteger getPasswordSaltS() {
        return passwordSaltS;
    }

    public BigInteger getServerPublicValueB() {
        return serverPublicValueB;
    }

}
