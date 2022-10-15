package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.agreement.jpake.JPAKERound1Payload;

import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class JPakeRound1PayloadSerde implements ISerde<JPAKERound1Payload> {

    public static final JPakeRound1PayloadSerde INSTANCE = new JPakeRound1PayloadSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(null);

    private JPakeRound1PayloadSerde() {}

    @Override
    public JPAKERound1Payload fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final JPAKERound1Payload obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public JPAKERound1Payload fromBuffer(final IByteBuffer buffer) {
        final int knowledgeProofForX1Length = buffer.getInt(0);
        final byte[][] arrays = delegate.fromBuffer(buffer.slice(Integer.BYTES, buffer.capacity() - Integer.BYTES));
        final String participantId = new String(arrays[0], Charsets.UTF_8);
        final BigInteger gx1 = new BigInteger(arrays[1]);
        final BigInteger gx2 = new BigInteger(arrays[2]);
        final BigInteger[] knowledgeProofForX1 = new BigInteger[knowledgeProofForX1Length];
        final BigInteger[] knowledgeProofForX2 = new BigInteger[arrays.length - knowledgeProofForX1Length - 3];
        for (int i = 0; i < knowledgeProofForX1.length; i++) {
            knowledgeProofForX1[i] = new BigInteger(arrays[3 + i]);
        }
        for (int i = 0; i < knowledgeProofForX2.length; i++) {
            knowledgeProofForX2[i] = new BigInteger(arrays[3 + knowledgeProofForX1Length + i]);
        }
        return new JPAKERound1Payload(participantId, gx1, gx2, knowledgeProofForX1, knowledgeProofForX2);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final JPAKERound1Payload obj) {
        final String participantId = obj.getParticipantId();
        final BigInteger gx1 = obj.getGx1();
        final BigInteger gx2 = obj.getGx2();
        final BigInteger[] knowledgeProofForX1 = obj.getKnowledgeProofForX1();
        final BigInteger[] knowledgeProofForX2 = obj.getKnowledgeProofForX2();

        final byte[] participantIdBytes = participantId.getBytes(Charsets.UTF_8);
        final byte[] gx1Bytes = gx1.toByteArray();
        final byte[] gx2Bytes = gx2.toByteArray();

        final int knowledgeProofForX1Length = knowledgeProofForX1.length;
        final byte[][] arrays = new byte[3 + knowledgeProofForX1Length + knowledgeProofForX2.length][];
        arrays[0] = participantIdBytes;
        arrays[1] = gx1Bytes;
        arrays[2] = gx2Bytes;
        for (int i = 0; i < knowledgeProofForX1Length; i++) {
            arrays[3 + i] = knowledgeProofForX1[i].toByteArray();
        }
        for (int i = 0; i < knowledgeProofForX2.length; i++) {
            arrays[3 + knowledgeProofForX1Length + i] = knowledgeProofForX2[i].toByteArray();
        }
        buffer.putInt(0, knowledgeProofForX1Length);
        return delegate.toBuffer(buffer.sliceFrom(Integer.BYTES), arrays) + Integer.BYTES;
    }

}
