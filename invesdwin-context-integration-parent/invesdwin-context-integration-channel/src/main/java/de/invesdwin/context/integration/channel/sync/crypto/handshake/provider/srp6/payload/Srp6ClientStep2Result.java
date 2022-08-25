package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Srp6ClientStep2Result {

    private final BigInteger clientPublicValueA;
    private final BigInteger clientEvidenceMessageM1;

    public Srp6ClientStep2Result(final BigInteger clientPublicValueA, final BigInteger clientEvidenceMessageM1) {
        this.clientPublicValueA = clientPublicValueA;
        this.clientEvidenceMessageM1 = clientEvidenceMessageM1;
    }

    public BigInteger getClientPublicValueA() {
        return clientPublicValueA;
    }

    public BigInteger getClientEvidenceMessageM1() {
        return clientEvidenceMessageM1;
    }

}
