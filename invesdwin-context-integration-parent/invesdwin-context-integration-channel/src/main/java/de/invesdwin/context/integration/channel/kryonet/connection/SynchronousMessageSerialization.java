package de.invesdwin.context.integration.channel.kryonet.connection;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.KryoSerialization;
import com.esotericsoftware.kryonet.Serialization;

import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.util.lang.buffer.ByteBuffers;
import de.invesdwin.util.math.Booleans;
import de.invesdwin.util.math.Bytes;

@Immutable
public final class SynchronousMessageSerialization implements Serialization {

    public static final SynchronousMessageSerialization INSTANCE = new SynchronousMessageSerialization();

    private static final int KRYO_INDEX = 0;
    private static final int KRYO_SIZE = Booleans.BYTES;

    private static final int TYPE_INDEX = KRYO_INDEX + KRYO_SIZE;
    private static final int TYPE_SIZE = Integer.SIZE;

    private static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    private static final int SEQUENCE_SIZE = Integer.SIZE;

    private static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private static final KryoSerialization DELEGATE = new KryoSerialization();

    private SynchronousMessageSerialization() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(final Connection connection, final ByteBuffer buffer, final Object object) {
        if (object instanceof ISynchronousCommand) {
            final ISynchronousCommand<byte[]> cObject = (ISynchronousCommand<byte[]>) object;
            final int position = ByteBuffers.wrap(buffer).putBoolean(0, false);
            buffer.position(position);
            buffer.putInt(cObject.getType());
            buffer.putInt(cObject.getSequence());
            final byte[] message = cObject.getMessage();
            if (message != null && message.length > 0) {
                buffer.put(message);
            }
        } else {
            final int position = ByteBuffers.wrap(buffer).putBoolean(0, true);
            buffer.position(position);
            DELEGATE.write(connection, buffer, object);
        }
    }

    @Override
    public Object read(final Connection connection, final ByteBuffer buffer) {
        final boolean kryo = ByteBuffers.wrap(buffer).getBoolean(0);
        if (kryo) {
            return DELEGATE.read(connection, buffer);
        } else {
            final int type = buffer.getInt();
            final int sequence = buffer.getInt();
            final int size = buffer.remaining();
            final byte[] message;
            if (size <= 0) {
                message = Bytes.EMPTY_ARRAY;
            } else {
                message = new byte[size];
                buffer.get(message);
            }
            return new ImmutableSynchronousCommand<byte[]>(type, sequence, message);
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
