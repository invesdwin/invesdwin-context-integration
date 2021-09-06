package de.invesdwin.context.integration.channel.sync.kryonet.connection;

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
    public void write(final Connection connection, final java.nio.ByteBuffer buffer, final Object object) {
        final IByteBuffer wrapped = ByteBuffers.wrap(buffer);
        final int position = buffer.position();
        if (object instanceof IByteBufferWriter) {
            final IByteBufferWriter cObject = (IByteBufferWriter) object;
            wrapped.putBoolean(position + KRYO_INDEX, false);
            final int length = cObject.write(wrapped.sliceFrom(position + MESSAGE_INDEX));
            ByteBuffers.position(buffer, position + length + MESSAGE_INDEX);
        } else {
            wrapped.putBoolean(position + KRYO_INDEX, true);
            ByteBuffers.position(buffer, position + MESSAGE_INDEX);
            DELEGATE.write(connection, buffer, object);
        }
    }

    @Override
    public Object read(final Connection connection, final java.nio.ByteBuffer buffer) {
        final IByteBuffer wrapped = ByteBuffers.wrap(buffer);
        final int position = buffer.position();
        final boolean kryo = wrapped.getBoolean(position + KRYO_INDEX);
        if (kryo) {
            ByteBuffers.position(buffer, position + MESSAGE_INDEX);
            return DELEGATE.read(connection, buffer);
        } else {
            //since each message is read asynchronously we need to create a snapshot of the byte array here (sadly)
            //we could use a serde here directly like in the netty channel, but kryonet is not worth the effort for zero-copy
            final int length = buffer.limit() - position;
            final IByteBuffer copy = wrapped.clone(position + MESSAGE_INDEX, length - MESSAGE_INDEX);
            ByteBuffers.position(buffer, position + length);
            return copy;
        }
    }

    @Override
    public int getLengthLength() {
        return Integer.BYTES;
    }

    @Override
    public void writeLength(final java.nio.ByteBuffer buffer, final int length) {
        buffer.putInt(length);
    }

    @Override
    public int readLength(final java.nio.ByteBuffer buffer) {
        return buffer.getInt();
    }

}
