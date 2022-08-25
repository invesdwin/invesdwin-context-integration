package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound2Payload;

import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public final class JPakeRound2PayloadSerde extends RemoteFastSerializingSerde<JPAKERound2Payload> {

    public static final JPakeRound2PayloadSerde INSTANCE = new JPakeRound2PayloadSerde();

    private JPakeRound2PayloadSerde() {
        super(false, JPAKERound2Payload.class);
    }

}
