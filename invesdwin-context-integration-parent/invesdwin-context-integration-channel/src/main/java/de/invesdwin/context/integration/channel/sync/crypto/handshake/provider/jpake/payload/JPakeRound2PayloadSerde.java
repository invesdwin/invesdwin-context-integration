package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound2Payload;

import de.invesdwin.util.lang.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class JPakeRound2PayloadSerde implements ISerde<JPAKERound2Payload> {

    public static final JPakeRound2PayloadSerde INSTANCE = new JPakeRound2PayloadSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(null);

    private JPakeRound2PayloadSerde() {}

    @Override
    public JPAKERound2Payload fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final JPAKERound2Payload obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public JPAKERound2Payload fromBuffer(final IByteBuffer buffer, final int length) {
        final byte[][] arrays = delegate.fromBuffer(buffer, length);
        final String participantId = new String(arrays[0], Charsets.UTF_8);
        final BigInteger a = new BigInteger(arrays[1]);
        final BigInteger[] knowledgeProofForX2s = new BigInteger[arrays.length - 2];
        for (int i = 0; i < knowledgeProofForX2s.length; i++) {
            knowledgeProofForX2s[i] = new BigInteger(arrays[i + 2]);
        }
        return new JPAKERound2Payload(participantId, a, knowledgeProofForX2s);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final JPAKERound2Payload obj) {
        final String participantId = obj.getParticipantId();
        final BigInteger a = obj.getA();
        final BigInteger[] knowledgeProofForX2s = obj.getKnowledgeProofForX2s();

        final byte[] participantIdBytes = participantId.getBytes(Charsets.UTF_8);
        final byte[] aBytes = a.toByteArray();

        final byte[][] arrays = new byte[2 + knowledgeProofForX2s.length][];
        arrays[0] = participantIdBytes;
        arrays[1] = aBytes;
        for (int i = 0; i < knowledgeProofForX2s.length; i++) {
            arrays[i + 2] = knowledgeProofForX2s[i].toByteArray();
        }
        return delegate.toBuffer(buffer, arrays);
    }

}
