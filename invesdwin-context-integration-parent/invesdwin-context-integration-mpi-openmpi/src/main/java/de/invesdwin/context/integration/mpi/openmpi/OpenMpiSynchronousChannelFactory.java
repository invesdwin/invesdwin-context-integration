package de.invesdwin.context.integration.mpi.openmpi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiSynchronousChannelFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class OpenMpiSynchronousChannelFactory implements IMpiSynchronousChannelFactory {

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcast() {
        return null;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSend() {
        return null;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReceive() {
        return null;
    }

}
