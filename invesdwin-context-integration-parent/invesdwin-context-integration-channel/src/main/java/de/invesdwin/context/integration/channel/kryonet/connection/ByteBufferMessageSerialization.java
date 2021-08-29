package de.invesdwin.context.integration.channel.kryonet.connection;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.KryoSerialization;
import com.esotericsoftware.kryonet.Serialization;

import de.invesdwin.util.math.Booleans;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@Immutable
public final class ByteBufferMessageSerialization implements Serialization {

    public static final ByteBufferMessageSerialization INSTANCE = new ByteBufferMessageSerialization();

    private static final int KRYO_INDEX = 0;
    private static final int KRYO_SIZE = Booleans.BYTES;

    private static final int MESSAGE_INDEX = KRYO_INDEX + KRYO_SIZE;

    private static final KryoSerialization DELEGATE = new KryoSerialization();

    private ByteBufferMessageSerialization() {
    }

    @Override
    public void write(final Connection connection, final ByteBuffer buffer, final Object object) {
        final IByteBuffer wrapped = ByteBuffers.wrap(buffer);
        if (object instanceof IByteBufferWriter) {
            final IByteBufferWriter cObject = (IByteBufferWriter) object;
            wrapped.putBoolean(KRYO_INDEX, false);
            cObject.write(wrapped.sliceFrom(MESSAGE_INDEX));
        } else {
            wrapped.putBoolean(0, true);
            buffer.position(MESSAGE_INDEX);
            DELEGATE.write(connection, buffer, object);
        }
    }

    @Override
    public Object read(final Connection connection, final ByteBuffer buffer) {
        final IByteBuffer wrapped = ByteBuffers.wrap(buffer);
        final boolean kryo = wrapped.getBoolean(KRYO_INDEX);
        if (kryo) {
            buffer.position(MESSAGE_INDEX);
            return DELEGATE.read(connection, buffer);
        } else {
            //since each message is read asynchronously we need to create a snapshot of the byte array here (sadly)
            return ByteBuffers.wrap(wrapped.asByteArrayCopyFrom(MESSAGE_INDEX));
        }
    }

    @Override
    public int getLengthLength() {
        return Integer.BYTES;
    }

    @Override
    public void writeLength(final ByteBuffer buffer, final int length) {
        buffer.putInt(length);
    }

    @Override
    public int readLength(final ByteBuffer buffer) {
        return buffer.getInt();
    }

}
