package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class Srp6ServerStep1ResultSerde implements ISerde<Srp6ServerStep1Result> {

    public static final Srp6ServerStep1ResultSerde INSTANCE = new Srp6ServerStep1ResultSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(2);

    private Srp6ServerStep1ResultSerde() {}

    @Override
    public Srp6ServerStep1Result fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final Srp6ServerStep1Result obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public Srp6ServerStep1Result fromBuffer(final IByteBuffer buffer, final int length) {
        final byte[][] arrays = delegate.fromBuffer(buffer, length);

        final BigInteger passwordSaltS = new BigInteger(arrays[0]);
        final BigInteger passwordVerifierV = new BigInteger(arrays[1]);

        return new Srp6ServerStep1Result(passwordSaltS, passwordVerifierV);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final Srp6ServerStep1Result obj) {
        final BigInteger passwordSaltS = obj.getPasswordSaltS();
        final BigInteger serverPublicValueB = obj.getServerPublicValueB();

        final byte[] passwordSaltSBytes = passwordSaltS.toByteArray();
        final byte[] serverPublicValueBBytes = serverPublicValueB.toByteArray();

        final byte[][] arrays = new byte[][] { passwordSaltSBytes, serverPublicValueBBytes };

        return delegate.toBuffer(buffer, arrays);
    }

}
