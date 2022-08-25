package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class Srp6ServerStep1LookupInputSerde implements ISerde<Srp6ServerStep1LookupInput> {

    public static final Srp6ServerStep1LookupInputSerde INSTANCE = new Srp6ServerStep1LookupInputSerde();

    private Srp6ServerStep1LookupInputSerde() {}

    @Override
    public Srp6ServerStep1LookupInput fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final Srp6ServerStep1LookupInput obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public Srp6ServerStep1LookupInput fromBuffer(final IByteBuffer buffer, final int length) {
        final String userId = buffer.getStringUtf8(0, length);
        return new Srp6ServerStep1LookupInput(userId);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final Srp6ServerStep1LookupInput obj) {
        final String userId = obj.getUserIdHash();
        final int userIdLength = buffer.putStringUtf8(0, userId);
        return userIdLength;
    }

}
