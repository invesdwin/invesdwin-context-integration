package de.invesdwin.context.integration.mpi.mpjexpress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiSynchronousChannelFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class MpjExpressSynchronousChannelFactory implements IMpiSynchronousChannelFactory {

    @Override
    public ISynchronousReader<IByteBufferProvider> newISend() {
        return null;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newIReceive() {
        return null;
    }

}
