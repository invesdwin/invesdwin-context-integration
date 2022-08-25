package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;

@Immutable
public class Srp6ServerStep2Result extends AValueObject {

    private final BigInteger serverEvidenceMessageM2;

    public Srp6ServerStep2Result(final BigInteger serverEvidenceMessageM2) {
        this.serverEvidenceMessageM2 = serverEvidenceMessageM2;
    }

    public BigInteger getServerEvidenceMessageM2() {
        return serverEvidenceMessageM2;
    }

}
