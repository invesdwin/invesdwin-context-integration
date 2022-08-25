package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class Srp6ClientStep2ResultSerde implements ISerde<Srp6ClientStep2Result> {

    public static final Srp6ClientStep2ResultSerde INSTANCE = new Srp6ClientStep2ResultSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(2);

    private Srp6ClientStep2ResultSerde() {}

    @Override
    public Srp6ClientStep2Result fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final Srp6ClientStep2Result obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public Srp6ClientStep2Result fromBuffer(final IByteBuffer buffer, final int length) {
        final byte[][] arrays = delegate.fromBuffer(buffer, length);

        final BigInteger clientPublicValueA = new BigInteger(arrays[0]);
        final BigInteger clientEvidenceMessageM1 = new BigInteger(arrays[1]);

        return new Srp6ClientStep2Result(clientPublicValueA, clientEvidenceMessageM1);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final Srp6ClientStep2Result obj) {
        final BigInteger clientPublicValueA = obj.getClientPublicValueA();
        final BigInteger clientEvidenceMessageM1 = obj.getClientEvidenceMessageM1();

        final byte[] clientPublicValueABytes = clientPublicValueA.toByteArray();
        final byte[] clientEvidenceMessageM1Bytes = clientEvidenceMessageM1.toByteArray();

        final byte[][] arrays = new byte[][] { clientPublicValueABytes, clientEvidenceMessageM1Bytes };
        return delegate.toBuffer(buffer, arrays);
    }

}
