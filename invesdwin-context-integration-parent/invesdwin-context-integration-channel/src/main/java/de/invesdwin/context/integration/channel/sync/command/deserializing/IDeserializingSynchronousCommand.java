package de.invesdwin.context.integration.channel.sync.command.deserializing;

import de.invesdwin.context.integration.channel.sync.command.ISynchronousCommand;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface IDeserializingSynchronousCommand<M> extends ISynchronousCommand<M> {

    void setType(int type);

    void setSequence(int sequence);

    void setMessage(ISerde<M> messageSerde, IByteBuffer message);

}
