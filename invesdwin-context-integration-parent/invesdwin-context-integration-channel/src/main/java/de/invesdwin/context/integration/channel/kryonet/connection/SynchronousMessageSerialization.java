package de.invesdwin.context.integration.channel.kryonet.connection;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Serialization;

import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;
import de.invesdwin.util.math.Bytes;

@Immutable
public final class SynchronousMessageSerialization implements Serialization {

    public static final SynchronousMessageSerialization INSTANCE = new SynchronousMessageSerialization();

    private static final int TYPE_INDEX = 0;
    private static final int TYPE_SIZE = Integer.SIZE;

    private static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    private static final int SEQUENCE_SIZE = Integer.SIZE;

    private static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private SynchronousMessageSerialization() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(final Connection connection, final ByteBuffer buffer, final Object object) {
        final ISynchronousMessage<byte[]> cObject = (ISynchronousMessage<byte[]>) object;
        buffer.putInt(TYPE_INDEX, cObject.getType());
        buffer.putInt(SEQUENCE_INDEX, cObject.getSequence());
        final byte[] message = cObject.getMessage();
        if (message != null && message.length > 0) {
            buffer.put(MESSAGE_INDEX, message);
        }
    }

    @Override
    public Object read(final Connection connection, final ByteBuffer buffer) {
        final int type = buffer.getInt(TYPE_INDEX);
        final int sequence = buffer.getInt(SEQUENCE_INDEX);
        final int size = buffer.remaining() - MESSAGE_INDEX;
        final byte[] message;
        if (size <= 0) {
            message = Bytes.EMPTY_ARRAY;
        } else {
            message = new byte[size];
            buffer.get(MESSAGE_INDEX, message);
        }
        return new ImmutableSynchronousMessage<byte[]>(type, sequence, message);
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
