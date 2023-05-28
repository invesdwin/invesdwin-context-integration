package de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing;

import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface IDeserializingServiceSynchronousCommand<M> extends IServiceSynchronousCommand<M> {

    void setService(int service);

    void setMethod(int method);

    void setSequence(int sequence);

    void setMessage(ISerde<M> messageSerde, IByteBuffer message);

    @Override
    void close();

}
