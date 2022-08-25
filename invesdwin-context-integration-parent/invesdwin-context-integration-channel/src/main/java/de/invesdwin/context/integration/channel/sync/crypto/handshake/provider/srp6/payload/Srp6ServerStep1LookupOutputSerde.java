package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.ByteArraySerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class Srp6ServerStep1LookupOutputSerde implements ISerde<Srp6ServerStep1LookupOutput> {

    public static final Srp6ServerStep1LookupOutputSerde INSTANCE = new Srp6ServerStep1LookupOutputSerde();

    private final ByteArraySerde delegate = ByteArraySerde.getInstance(2);

    private Srp6ServerStep1LookupOutputSerde() {}

    @Override
    public Srp6ServerStep1LookupOutput fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final Srp6ServerStep1LookupOutput obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public Srp6ServerStep1LookupOutput fromBuffer(final IByteBuffer buffer, final int length) {
        final byte[][] arrays = delegate.fromBuffer(buffer, length);

        final BigInteger passwordSaltS = new BigInteger(arrays[0]);
        final BigInteger passwordVerifierV = new BigInteger(arrays[1]);

        return new Srp6ServerStep1LookupOutput(passwordSaltS, passwordVerifierV);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final Srp6ServerStep1LookupOutput obj) {
        final BigInteger passwordSaltS = obj.getPasswordSaltS();
        final BigInteger passwordVerifierV = obj.getPasswordVerifierV();

        final byte[] passwordSaltSBytes = passwordSaltS.toByteArray();
        final byte[] passwordVerifierVBytes = passwordVerifierV.toByteArray();

        final byte[][] arrays = new byte[][] { passwordSaltSBytes, passwordVerifierVBytes };

        return delegate.toBuffer(buffer, arrays);
    }

}
