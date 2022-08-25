package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound1Payload;

import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public final class JPakeRound1PayloadSerde extends RemoteFastSerializingSerde<JPAKERound1Payload> {

    public static final JPakeRound1PayloadSerde INSTANCE = new JPakeRound1PayloadSerde();

    private JPakeRound1PayloadSerde() {
        super(false, JPAKERound1Payload.class);
    }

}
