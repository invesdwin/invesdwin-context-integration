package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class Srp6ServerStep2ResultSerde implements ISerde<Srp6ServerStep2Result> {

    public static final Srp6ServerStep2ResultSerde INSTANCE = new Srp6ServerStep2ResultSerde();

    private Srp6ServerStep2ResultSerde() {}

    @Override
    public Srp6ServerStep2Result fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final Srp6ServerStep2Result obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public Srp6ServerStep2Result fromBuffer(final IByteBuffer buffer) {
        final BigInteger serverEvidenceMessageM2 = new BigInteger(buffer.asByteArray());
        return new Srp6ServerStep2Result(serverEvidenceMessageM2);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final Srp6ServerStep2Result obj) {
        final BigInteger serverEvidenceMessageM2 = obj.getServerEvidenceMessageM2();
        final byte[] serverEvidenceMessageM2Bytes = serverEvidenceMessageM2.toByteArray();
        buffer.putBytes(0, serverEvidenceMessageM2Bytes);
        return serverEvidenceMessageM2Bytes.length;
    }

}
