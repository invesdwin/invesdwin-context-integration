package de.invesdwin.context.integration.channel.sync.command;

import java.io.Closeable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface ISynchronousCommand<M> extends Closeable {

    int getType();

    int getSequence();

    M getMessage();

    default int messageToBuffer(final ISerde<M> messageSerde, final IByteBuffer buffer) {
        return messageSerde.toBuffer(buffer, getMessage());
    }

}
