package de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

public interface ISerializingServiceSynchronousCommand<M> extends IServiceSynchronousCommand<IByteBufferProvider> {

    void setService(int service);

    void setMethod(int method);

    void setSequence(int sequence);

    void setMessage(ISerde<M> messageSerde, M message);

    void setMessageBuffer(IByteBufferProvider messageBuffer);

    void setCloseableMessageBuffer(ICloseableByteBufferProvider messageBuffer);

    @Override
    void close();

}
