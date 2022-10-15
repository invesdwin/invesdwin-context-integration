package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound3Payload;

import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class JPakeRound3PayloadSerde implements ISerde<JPAKERound3Payload> {

    public static final JPakeRound3PayloadSerde INSTANCE = new JPakeRound3PayloadSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(2);

    private JPakeRound3PayloadSerde() {}

    @Override
    public JPAKERound3Payload fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final JPAKERound3Payload obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public JPAKERound3Payload fromBuffer(final IByteBuffer buffer) {
        final byte[][] arrays = delegate.fromBuffer(buffer);

        final String participantId = new String(arrays[0], Charsets.UTF_8);
        final BigInteger a = new BigInteger(arrays[1]);

        return new JPAKERound3Payload(participantId, a);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final JPAKERound3Payload obj) {
        final String participantId = obj.getParticipantId();
        final BigInteger macTag = obj.getMacTag();

        final byte[] participantIdBytes = participantId.getBytes(Charsets.UTF_8);
        final byte[] macTagBytes = macTag.toByteArray();

        final byte[][] arrays = new byte[][] { participantIdBytes, macTagBytes };

        return delegate.toBuffer(buffer, arrays);
    }

}
