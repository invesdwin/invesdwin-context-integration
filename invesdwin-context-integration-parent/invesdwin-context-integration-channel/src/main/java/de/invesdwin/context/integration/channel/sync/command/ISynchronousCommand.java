package de.invesdwin.context.integration.channel.sync.command;

import java.io.Closeable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface ISynchronousCommand<M> extends Closeable {

    int getType();

    int getSequence();

    M getMessage();

    default int toBuffer(final ISerde<M> messageSerde, final IByteBuffer buffer) {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, getType());
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, getSequence());
        final int messageLength = messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                getMessage());
        return SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
    }

}
