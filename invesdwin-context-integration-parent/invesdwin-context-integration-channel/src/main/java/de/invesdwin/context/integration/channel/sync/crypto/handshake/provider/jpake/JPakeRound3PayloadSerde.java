package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound3Payload;

import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public final class JPakeRound3PayloadSerde extends RemoteFastSerializingSerde<JPAKERound3Payload> {

    public static final JPakeRound3PayloadSerde INSTANCE = new JPakeRound3PayloadSerde();

    private JPakeRound3PayloadSerde() {
        super(false, JPAKERound3Payload.class);
    }

}
