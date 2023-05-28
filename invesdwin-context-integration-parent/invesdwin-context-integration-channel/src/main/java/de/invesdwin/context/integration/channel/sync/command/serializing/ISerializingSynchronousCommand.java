package de.invesdwin.context.integration.channel.sync.command.serializing;

import de.invesdwin.context.integration.channel.sync.command.ISynchronousCommand;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface ISerializingSynchronousCommand<M> extends ISynchronousCommand<IByteBufferProvider> {

    void setType(int type);

    void setSequence(int sequence);

    void setMessage(ISerde<M> messageSerde, M message);

}
